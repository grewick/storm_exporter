FROM alpine:3.5
RUN apk add --no-cache python3 curl && \
    python3 -m ensurepip && \
    rm -r /usr/lib/python*/ensurepip && \
    pip3 install --upgrade pip prometheus_client requests\
    && rm -rf /var/cache/apk/*

WORKDIR /app
COPY storm_exporter.py /app

EXPOSE 8000

ENTRYPOINT [ "/bin/sh", "-c", "python3 /app/storm_exporter.py $STORM_UI_HOST 8000 $REFRESH_RATE" ]
