FROM python:3.13-slim

RUN useradd --system --uid 10001 inverter && mkdir /data && chown inverter /data
WORKDIR /app
COPY logger.py index.html ./

ENV HTTP_HOST=0.0.0.0 \
    DB_PATH=/data/inverter.sqlite \
    POLL_INTERVAL=5 \
    PYTHONUNBUFFERED=1

USER inverter
VOLUME /data
# 18899: the Wi-Fi dongle connects here; 8090: dashboard
EXPOSE 18899 8090
CMD ["python", "logger.py"]
