"""Local logger and control panel for a JSDSolar J4000HC inverter behind an Eybond Wi-Fi dongle.

The dongle is a TCP client: it connects to the host set by AT+CLDSRVHOST1 and then waits for
requests. We are the master. The inverter speaks the JsdSolar "G-command" ASCII protocol
(GPDAT0, GBAT, ... terminated by CR), which we send through the dongle's fc4 passthrough.
Command names, field positions and setting ranges come from
https://github.com/groove-max/ha-eybond-local (eybond_g_ascii).

Usage: python logger.py [poll_interval_seconds]
Dashboard: http://127.0.0.1:8090/

Environment: LISTEN_PORT, HTTP_HOST, HTTP_PORT, DB_PATH, POLL_INTERVAL, AUTH_USER, AUTH_PASSWORD.
The dashboard can change inverter settings, so it only listens beyond localhost when
AUTH_PASSWORD is set (HTTP Basic auth; put HTTPS in front of it on a public server).
"""
import base64
import hmac
import json
import os
import queue
import socket
import sqlite3
import struct
import sys
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

LISTEN_PORT = int(os.environ.get('LISTEN_PORT', 18899))
HTTP_HOST = os.environ.get('HTTP_HOST', '127.0.0.1')
HTTP_PORT = int(os.environ.get('HTTP_PORT', 8090))
AUTH_USER = os.environ.get('AUTH_USER', 'admin')
AUTH_PASSWORD = os.environ.get('AUTH_PASSWORD', '')
HISTORY_POINTS = 600
INDEX_PATH = Path(__file__).with_name('index.html')
POLL_INTERVAL = float(os.environ.get('POLL_INTERVAL', 2.0))
HEARTBEAT_INTERVAL = 60
# The dongle answers one request at a time and sometimes takes seconds. Giving up early and
# sending the next request only queues it behind the slow one until the dongle stops answering.
REQUEST_TIMEOUT = 10.0
WRITE_WAIT = 40.0
DB_PATH = Path(os.environ.get('DB_PATH') or Path(__file__).with_name('inverter.sqlite'))

MODES = {'0': 'Увімкнення', '1': 'Вимкнено', '2': 'Аварія', '3': 'Очікування',
         '4': 'Від мережі', '5': 'Від батареї', '6': 'Тест'}

# GPDAT0 reply: field index -> name
LIVE_FIELDS = {
    5: 'inverter_voltage_v', 6: 'inverter_frequency_hz',
    7: 'grid_voltage_v', 8: 'grid_frequency_hz',
    9: 'out_voltage_v', 10: 'out_frequency_hz', 11: 'out_current_a',
    12: 'battery_voltage_v', 13: 'battery_current_a',
    14: 'load_pct', 15: 'out_apparent_va', 16: 'out_power_w',
    17: 'battery_soc_pct',
    18: 'pv_voltage_v', 19: 'pv_current_a', 20: 'pv_power_w',
    21: 'temperature_c',
}


def number(key, title, command, lo, hi, unit, width=3, precision=0):
    return {'key': key, 'title': title, 'command': command, 'type': 'number', 'min': lo, 'max': hi,
            'unit': unit, 'width': width, 'precision': precision, 'writable': True}


def choice(key, title, command, options, writable=False):
    width = len(next(iter(options)))
    return {'key': key, 'title': title, 'command': command, 'type': 'enum', 'options': options,
            'width': width, 'precision': 0, 'writable': writable}


# Only GCC has been written and read back on this inverter; the other numeric ranges are the
# ones documented for 24 V units, and the inverter itself answers NAK to what it rejects.
# Enums without a documented local write command stay read-only.
SETTINGS = [
    choice('output_priority', 'Пріоритет джерела виходу', 'OPR',
           {'00': 'Спочатку мережа', '01': 'Спочатку панелі', '02': 'Панелі > батарея > мережа'}),
    choice('output_mode', 'Режим виходу', 'OPM', {'00': 'Побутові прилади (APP)', '01': 'ДБЖ (UPS)'}),
    choice('charging_priority', 'Пріоритет заряду', 'CPR',
           {'00': 'Спочатку мережа', '01': 'Спочатку панелі', '02': 'Мережа + панелі', '03': 'Лише панелі'}),
    choice('battery_type', 'Тип батареї', 'TBAT',
           {'0': 'Свинцево-кислотна (AGM)', '1': 'Заливна (FLD)', '2': 'Літієва', '3': 'Користувацька'}),
    choice('charging_method', 'Метод заряду', 'CST',
           {'00': 'Авто', '01': 'Примусово 2 стадії', '02': 'Примусово 3 стадії'}, writable=True),
    number('max_charging_current', 'Макс. струм заряду', 'CHGC', 10, 120, 'А'),
    number('utility_charging_current', 'Струм заряду від мережі', 'GCC', 1, 120, 'А'),
    number('cv_voltage', 'Напруга заряду (CV)', 'TCCV', 28.0, 29.0, 'В', 4, 1),
    number('float_voltage', 'Плаваюча напруга', 'TCFV', 26.6, 27.8, 'В', 4, 1),
    number('cv_time', 'Час заряду CV', 'TCVT', 1, 720, 'хв', 4),
    number('cc_max_time', 'Макс. час заряду постійним струмом', 'CI1', 1, 99, 'год', 2),
    number('discharge_cutoff_voltage', 'Напруга відключення батареї', 'EOD', 20.0, 22.0, 'В', 4, 1),
    number('discharge_alarm_voltage', 'Напруга попередження про розряд', 'TBLV', 20.6, 22.6, 'В', 4, 1),
    number('battery_to_grid_voltage', 'Перехід з батареї на мережу', 'BTG', 22.0, 26.0, 'В', 4, 1),
    number('grid_to_battery_voltage', 'Повернення з мережі на батарею', 'BTB', 0.0, 29.0, 'В', 4, 1),
    number('battery_overvoltage', 'Захист від перенапруги батареї', 'BTO', 28.0, 32.0, 'В', 4, 1),
    number('grid_overvoltage', 'Верхня межа напруги мережі', 'OVP', 264, 280, 'В'),
    number('grid_undervoltage', 'Нижня межа напруги мережі', 'LVP', 90, 200, 'В'),
    number('equalization_voltage', 'Напруга вирівнювання', 'TCQV', 24.0, 30.0, 'В', 4, 1),
    number('equalization_time', 'Час вирівнювання', 'TCQT', 5, 900, 'хв', 4),
    number('equalization_timeout', 'Тайм-аут вирівнювання', 'TCQO', 5, 900, 'хв', 4),
    number('equalization_interval', 'Інтервал вирівнювання', 'TCQI', 24, 2160, 'год', 4),
    number('soc_shutdown', 'BMS: вимкнення при низькому SOC', 'BSOCU', 0, 50, '%'),
    number('soc_to_grid', 'BMS: перехід на мережу при SOC', 'BSOCG', 0, 90, '%'),
    number('soc_to_battery', 'BMS: повернення на батарею при SOC', 'BSOCB', 0, 100, '%'),
]
SETTINGS_BY_KEY = {s['key']: s for s in SETTINGS}
SETTINGS_EVERY = 3  # poll cycles between two setting reads; one full pass takes a few minutes

setting_values = {}  # key -> {'raw': str, 'ts': int}; shared with the dashboard thread
writes = queue.Queue()  # (setting, raw value, result dict, done event) from the dashboard thread


class Dongle:
    DEVCODE = 1
    FC_HEARTBEAT = 1
    FC_PASSTHROUGH = 4

    def __init__(self, conn: socket.socket, replaced=lambda: False):
        self.conn = conn
        self.replaced = replaced  # tells whether a newer connection is waiting
        self.buf = b''
        self.tid = 0
        self.stats = {'ok': 0, 'timeout': 0, 'late': 0, 'slowest_ms': 0}

    def _take_frame(self) -> tuple[int, bytes] | None:
        if len(self.buf) < 8:
            return None
        tid, _, wire_len, _, _ = struct.unpack('>HHHBB', self.buf[:8])
        end = 6 + wire_len
        if len(self.buf) < end:
            return None
        body, self.buf = self.buf[8:end], self.buf[end:]
        return tid, body

    def request(self, devaddr: int, fcode: int, data: bytes) -> bytes | None:
        """Send one Eybond frame and return the reply payload, or None on timeout."""
        self.tid = (self.tid + 1) & 0xFFFF
        self.conn.sendall(struct.pack('>HHHBB', self.tid, self.DEVCODE, len(data) + 2, devaddr, fcode) + data)
        self.conn.settimeout(0.5)  # short slices, to notice a replacement connection quickly
        sent = time.monotonic()
        while True:
            frame = self._take_frame()
            if frame:
                if frame[0] == self.tid:
                    self.stats['ok'] += 1
                    self.stats['slowest_ms'] = max(self.stats['slowest_ms'], round((time.monotonic() - sent) * 1000))
                    return frame[1]
                self.stats['late'] += 1  # reply to a request that already timed out
                continue
            if self.replaced():
                raise ConnectionError('replaced by a new connection')
            if time.monotonic() - sent >= REQUEST_TIMEOUT:
                self.stats['timeout'] += 1
                return None
            try:
                chunk = self.conn.recv(4096)
            except socket.timeout:
                continue
            if not chunk:
                raise ConnectionError('closed by dongle')
            self.buf += chunk

    def heartbeat(self) -> str | None:
        now = datetime.now()
        data = bytes([now.year - 2000, now.month, now.day, now.hour, now.minute, now.second])
        reply = self.request(0xFF, self.FC_HEARTBEAT, data + struct.pack('>H', HEARTBEAT_INTERVAL))
        return reply.decode('ascii', 'replace') if reply else None

    def command(self, text: str) -> str | None:
        """Send one G-command to the inverter; returns its reply without the framing '(' and CR."""
        reply = self.request(1, self.FC_PASSTHROUGH, text.encode('ascii') + b'\r')
        if reply is None:
            return None
        return reply.decode('ascii', 'replace').strip('\r\n ').lstrip('(')


def read_live(dongle: Dongle) -> dict | None:
    reply = dongle.command('GPDAT0')
    fields = reply.split() if reply else []
    if len(fields) <= max(LIVE_FIELDS):
        return None
    try:
        values = {name: float(fields[i]) for i, name in LIVE_FIELDS.items()}
    except ValueError:
        return None
    values['battery_power_w'] = round(values['battery_voltage_v'] * values['battery_current_a'])
    values['mode'] = MODES.get(fields[1], fields[1])
    return values


def format_value(setting: dict, value) -> str:
    """Validate a new value and render it the way the inverter expects it after the command name."""
    if setting['type'] == 'enum':
        if value not in setting['options']:
            raise ValueError('невідомий варіант')
        return value
    value = float(value)
    if not setting['min'] <= value <= setting['max']:
        raise ValueError(f"дозволено від {setting['min']} до {setting['max']} {setting['unit']}")
    if setting['precision']:
        return f"{value:0{setting['width']}.{setting['precision']}f}"
    if value != int(value):
        raise ValueError('потрібне ціле число')
    return f"{int(value):0{setting['width']}d}"


def read_setting(dongle: Dongle, setting: dict) -> str | None:
    reply = dongle.command(setting['command'] + '?' * setting['width'])
    if not reply or reply in ('NAK', 'NOA', 'ERCRC'):
        return None
    setting_values[setting['key']] = {'raw': reply, 'ts': int(time.time())}
    return reply


def apply_write(dongle: Dongle, setting: dict, raw: str, result: dict) -> None:
    reply = dongle.command(setting['command'] + raw)
    log(f"write {setting['command']}{raw}: {reply}")
    if reply != 'ACK':
        result['error'] = 'інвертор не відповів' if reply is None else f'інвертор відхилив значення ({reply})'
        return
    result['raw'] = read_setting(dongle, setting)
    if result['raw'] is None:
        result['error'] = 'записано, але перечитати значення не вдалося'


def settings_view() -> list[dict]:
    items = []
    for setting in SETTINGS:
        state = setting_values.get(setting['key'], {})
        raw = state.get('raw')
        item = {k: setting[k] for k in ('key', 'title', 'type', 'writable')}
        item.update(raw=raw, ts=state.get('ts'))
        if setting['type'] == 'enum':
            item['options'] = setting['options']
        else:
            item.update(min=setting['min'], max=setting['max'], unit=setting['unit'],
                        step=0.1 if setting['precision'] else 1)
            try:
                item['value'] = float(raw) if raw is not None else None
            except ValueError:
                item['value'] = None
        items.append(item)
    return items


def history(db: sqlite3.Connection, minutes: int) -> list[dict]:
    """Readings of the last `minutes`, averaged into at most HISTORY_POINTS buckets."""
    since = int(time.time()) - minutes * 60
    bucket = max(1, minutes * 60 // HISTORY_POINTS)
    sums: dict[int, dict[str, list[float]]] = {}
    for ts, data in db.execute('SELECT ts, data FROM readings WHERE ts >= ? ORDER BY ts', (since,)):
        slot = sums.setdefault(ts // bucket * bucket, {})
        for name, value in json.loads(data).items():
            if isinstance(value, (int, float)):
                slot.setdefault(name, []).append(value)
    return [{'ts': ts, **{name: round(sum(v) / len(v), 2) for name, v in slot.items()}}
            for ts, slot in sums.items()]


class Dashboard(BaseHTTPRequestHandler):
    def authorized(self) -> bool:
        """Check HTTP Basic credentials; answers 401 itself when they are missing or wrong."""
        if not AUTH_PASSWORD:
            return True
        expected = base64.b64encode(f'{AUTH_USER}:{AUTH_PASSWORD}'.encode()).decode()
        scheme, _, given = self.headers.get('Authorization', '').partition(' ')
        if scheme == 'Basic' and hmac.compare_digest(given.strip(), expected):
            return True
        if given:
            time.sleep(1)  # slow down password guessing
        # drain a small request body so the client reads the 401 instead of a connection reset
        self.rfile.read(min(int(self.headers.get('Content-Length') or 0), 65536))
        self.send_response(401)
        self.send_header('WWW-Authenticate', 'Basic realm="Inverter", charset="UTF-8"')
        self.send_header('Content-Length', '0')
        self.end_headers()
        return False

    def do_GET(self) -> None:
        if not self.authorized():
            return
        url = urlparse(self.path)
        if url.path == '/':
            return self.reply(INDEX_PATH.read_bytes(), 'text/html; charset=utf-8')
        if url.path == '/api/settings':
            return self.reply_json(settings_view())
        if url.path not in ('/api/latest', '/api/history'):
            return self.send_error(404)
        db = sqlite3.connect(DB_PATH)
        try:
            if url.path == '/api/latest':
                row = db.execute('SELECT ts, data FROM readings ORDER BY ts DESC LIMIT 1').fetchone()
                body = {'ts': row[0], **json.loads(row[1])} if row else None
            else:
                minutes = int(parse_qs(url.query).get('minutes', ['60'])[0])
                body = history(db, min(max(minutes, 1), 7 * 24 * 60))
        finally:
            db.close()
        self.reply_json(body)

    def do_POST(self) -> None:
        if not self.authorized():
            return
        # JSON-only so that a page on another site cannot submit a form here
        if urlparse(self.path).path != '/api/settings' or self.headers.get('Content-Type') != 'application/json':
            return self.send_error(404)
        try:
            body = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))))
            setting = SETTINGS_BY_KEY[body['key']]
            if not setting['writable']:
                raise ValueError('це налаштування лише для читання')
            raw = format_value(setting, body['value'])
        except (KeyError, TypeError, ValueError) as e:
            return self.reply_json({'error': str(e) or 'некоректний запит'}, 400)
        result, done = {}, threading.Event()
        writes.put((setting, raw, result, done))
        if not done.wait(WRITE_WAIT):
            result['cancelled'] = True  # never apply it later, once the user has been told it failed
            return self.reply_json({'error': 'немає зв\'язку з інвертором'}, 502)
        self.reply_json(result, 502 if 'error' in result else 200)

    def reply_json(self, body, status: int = 200) -> None:
        self.reply(json.dumps(body).encode(), 'application/json', status)

    def reply(self, body: bytes, content_type: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        try:
            self.wfile.write(body)
        except OSError:
            pass  # the browser went away

    def log_message(self, *args) -> None:
        pass


def log(message: str) -> None:
    print(datetime.now().strftime('%H:%M:%S'), message, flush=True)


def accept_loop(srv: socket.socket, incoming: queue.Queue) -> None:
    """Hand every new connection to the polling loop and cut the previous one off.

    There is one dongle, so a new connection means the old one is dead even if no FIN arrived.
    Without this the poller keeps waiting on the dead socket while the dongle gives up on the
    queued one, and every connection is already closed by the time it gets served.
    """
    active = None
    while True:
        conn, peer = srv.accept()
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if active is not None:
            try:
                active.shutdown(socket.SHUT_RDWR)  # wakes the poller blocked in recv()
            except OSError:
                pass
        active = conn
        incoming.put((conn, peer))


def serve(db: sqlite3.Connection, interval: float) -> None:
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('0.0.0.0', LISTEN_PORT))
    srv.listen(8)
    incoming = queue.Queue()
    threading.Thread(target=accept_loop, args=(srv, incoming), daemon=True).start()
    log(f'listening on :{LISTEN_PORT}, poll every {interval}s, db {DB_PATH}')
    while True:
        conn, peer = incoming.get()
        if not incoming.empty():  # already replaced by a newer connection
            conn.close()
            continue
        log(f'dongle connected from {peer[0]}')
        dongle = Dongle(conn, replaced=lambda: not incoming.empty())
        connected = time.monotonic()
        last_heartbeat = 0.0
        cycle = 0
        try:
            while True:
                cycle += 1
                started = time.monotonic()
                if started - last_heartbeat >= HEARTBEAT_INTERVAL / 2:
                    log(f'heartbeat: {dongle.heartbeat()}, requests: {dongle.stats}')
                    last_heartbeat = started
                while not writes.empty():
                    setting, raw, result, done = writes.get()
                    try:
                        if not result.get('cancelled'):
                            apply_write(dongle, setting, raw, result)
                    finally:
                        done.set()
                values = read_live(dongle)
                if values:
                    db.execute('INSERT INTO readings (ts, data) VALUES (?, ?)', (int(time.time()), json.dumps(values)))
                    db.commit()
                    log(values)
                unread = [s for s in SETTINGS if s['key'] not in setting_values]
                if unread:  # first pass after start: one per cycle until the table is complete
                    read_setting(dongle, unread[cycle % len(unread)])
                elif cycle % SETTINGS_EVERY == 0:
                    read_setting(dongle, SETTINGS[cycle // SETTINGS_EVERY % len(SETTINGS)])
                time.sleep(max(0.0, interval - (time.monotonic() - started)))
        except (ConnectionError, OSError) as e:
            log(f'dongle disconnected after {time.monotonic() - connected:.1f}s: {e}, requests: {dongle.stats}')
        finally:
            conn.close()


def main() -> None:
    interval = float(sys.argv[1]) if len(sys.argv) > 1 else POLL_INTERVAL
    if not AUTH_PASSWORD and HTTP_HOST not in ('127.0.0.1', 'localhost', '::1'):
        sys.exit('Refusing to expose the dashboard without a password: set AUTH_PASSWORD or HTTP_HOST=127.0.0.1')
    db = sqlite3.connect(DB_PATH)
    db.execute('CREATE TABLE IF NOT EXISTS readings (ts INTEGER NOT NULL, data TEXT NOT NULL)')
    db.execute('CREATE INDEX IF NOT EXISTS readings_ts ON readings (ts)')
    db.commit()
    http = ThreadingHTTPServer((HTTP_HOST, HTTP_PORT), Dashboard)
    threading.Thread(target=http.serve_forever, daemon=True).start()
    log(f'dashboard on http://{HTTP_HOST}:{HTTP_PORT}/ ({"password protected" if AUTH_PASSWORD else "no password"})')
    try:
        serve(db, interval)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
