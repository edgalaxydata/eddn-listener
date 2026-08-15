FROM python:3-alpine

# echo $(( 0x$(echo 'EDDNListener' | sha256sum | head -c4) ))
ARG APPUID=38688
ARG APPGID=38688

RUN addgroup -g ${APPGID} app && \
    adduser -D -u ${APPUID} -G app app

RUN mkdir -p /srv/EDDN/data /srv/EDDN/log

RUN apk add --no-cache rsync ssh

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN chown -R app:app /app
RUN chown -R app:app /srv/EDDN

USER app

COPY msgtypes.txt /srv/EDDN/data
COPY . .

ENV EDDN_RELAY=tcp://eddn.edcd.io:9500
ENV EDDN_DIR=/srv/EDDN/data
ENV EDDN_LOGPREFIX=/srv/EDDN/log/eddn-live

STOPSIGNAL SIGINT

ENTRYPOINT [ "python", "/app/eddn_logger.py"]
