#!/usr/bin/python3

import zlib
import zmq
import json
import sys
import time
from datetime import datetime
import os
import os.path
import re
import signal
from urllib.parse import quote
import logging
from logging.handlers import TimedRotatingFileHandler, StreamHandler


RELAY = 'tcp://eddn.edcd.io:9500'
TIMEOUT = 60000
SCHEMABASE = 'https://eddn.edcd.io/schemas/'
EDDNDIR = '/srv/EDDN/data'
LOGPREFIX = '/srv/EDDN/log/eddn-live'

logger = logging.getLogger(__name__)


eddnfiles = {}
eddnfilenames = {}

message_types = {
    'https://eddn.edcd.io/schemas/journal/1': 'Journal',
    'https://eddn.edcd.io/schemas/outfitting/2': 'Outfitting',
    'https://eddn.edcd.io/schemas/shipyard/2': 'Shipyard',
    'https://eddn.edcd.io/schemas/commodity/3': 'Commodity',
    'https://eddn.edcd.io/schemas/blackmarket/1': 'BlackMarket',
    'https://eddn.edcd.io/schemas/journal/1/live': 'Journal',
    'https://eddn.edcd.io/schemas/outfitting/2/live': 'Outfitting',
    'https://eddn.edcd.io/schemas/shipyard/2/live': 'Shipyard',
    'https://eddn.edcd.io/schemas/commodity/3/live': 'Commodity',
    'https://eddn.edcd.io/schemas/blackmarket/1/live': 'BlackMarket',
    'https://eddn.edcd.io/schemas/journal/1/test': 'Test-Journal',
    'https://eddn.edcd.io/schemas/outfitting/2/test': 'Test-Outfitting',
    'https://eddn.edcd.io/schemas/shipyard/2/test': 'Test-Shipyard',
    'https://eddn.edcd.io/schemas/commodity/3/test': 'Test-Commodity',
    'https://eddn.edcd.io/schemas/blackmarket/1/test': 'Test-BlackMarket',
    'https://eddn.edcd.io/schemas/journal/1/beta': 'Beta-Journal',
    'https://eddn.edcd.io/schemas/outfitting/2/beta': 'Beta-Outfitting',
    'https://eddn.edcd.io/schemas/shipyard/2/beta': 'Beta-Shipyard',
    'https://eddn.edcd.io/schemas/commodity/3/beta': 'Beta-Commodity',
    'https://eddn.edcd.io/schemas/blackmarket/1/beta': 'Beta-BlackMarket',
}

message_types_last_read = 0

gwts_regex = re.compile(
    '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}'
)
msgevent_regex = re.compile(
    '^[A-Z][A-Za-z]*$'
)
eventtype_regex = re.compile(
    '^[A-Z][A-Za-z0-9._-]*[A-Za-z0-9]$'
)


def restart():
    logger.warning('Restarting on USR1')
    os.execv(sys.argv[0], sys.argv)


def handle_usr1(signum, stack):
    restart()


def process_msg(eddndir, msgtype, msg, msgraw):
    print(msgraw.decode('utf-8'))
    sys.stdout.flush()

    gwts = msg.get('header', {}).get('gatewayTimestamp')

    if gwts is None or not gwts_regex.match(gwts):
        logger.warning('Unrecognised gateway timestamp %s', gwts)
        gwts = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

    evttype = msg.get('message', {}).get('event')

    if msgtype not in ['Journal', 'Test-Journal']:
        eventtype = quote(msgtype)
    elif evttype is not None:
        eventtype = quote(msgtype + '.' + evttype)
    else:
        eventtype = 'Unknown'

    eddnname = f'{eddndir}/{eventtype}-{gwts[0:10]}.jsonl'

    eddnfile = eddnfiles.get(eventtype)
    prevname = eddnfilenames.get(eventtype, '')

    if eddnfile is not None and prevname != eddnname:
        eddnfile.close()
        eddnfile = None

    if eddnfile is None:
        eddnfilenames[eventtype] = eddnname
        eddnfile = open(eddnfilenames[eventtype], 'a', encoding='utf-8')
        eddnfiles[eventtype] = eddnfile

    eddnfile.write(json.dumps(msg, sort_keys=True))
    eddnfile.write('\n')
    eddnfile.flush()


def process_raw_msg(eddndir, message):
    try:
        message = zlib.decompress(message)
    except Exception:
        logger.exception(
            "Unable to decompress event",
            extra={'data': message.hex()}
        )

        return

    try:
        msg = message.decode('utf-8').strip()
    except Exception:
        logger.exception(
            "Unparseable event",
            extra={'data': message.decode('latin1')}
        )
        return

    try:
        msg = json.loads(msg)
    except Exception:
        logger.exception("Unparseable event: %s", msg)
        return

    schemaRef = msg.get('$schemaRef', '-')
    msgtype = message_types.get(schemaRef, 'Unknown')

    try:
        process_msg(eddndir, msgtype, msg, message)
    except AttributeError:
        logger.exception("Unhandled event: %s", json.dumps(msg))


def update_message_types(file):
    for line in file:
        schema, msgtype = line.strip().split()
        if schema.startswith(SCHEMABASE) and eventtype_regex.match(msgtype):
            message_types[schema] = msgtype


def read_message_types(eddndir):
    global message_types_last_read
    message_types_last_read = time.monotonic()

    try:
        with open(eddndir + '/msgtypes.txt', 'r', encoding='utf-8') as file:
            update_message_types(file)
    except Exception:
        pass


def setup_logging(logprefix):
    fileHandler = TimedRotatingFileHandler(
        filename=logprefix + ".log",
        when="midnight",
        interval=1,
        utc=True,
        encoding="utf-8"
    )

    fileHandler.suffix = "%Y-%m-%d"

    stderrHandler = StreamHandler(sys.stderr)

    formatter = logging.Formatter(
        "[%(asctime)s][%(levelname)s][%(name)s] %(message)s"
    )

    fileHandler.setFormatter(formatter)
    stderrHandler.setFormatter(formatter)

    logger.addHandler(fileHandler)
    logger.addHandler(stderrHandler)

    logger.setLevel(logging.INFO)


def main():
    relay = os.environ.get('EDDN_RELAY') or RELAY
    eddndir = os.environ.get('EDDN_DIR') or EDDNDIR
    logprefix = os.environ.get('EDDN_LOGPREFIX') or LOGPREFIX

    try:
        timeout = int(os.environ.get('EDDN_TIMEOUT')) or TIMEOUT
    except Exception:
        timeout = TIMEOUT

    setup_logging(logprefix)

    context = zmq.Context.instance()
    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.SUBSCRIBE, b'')
    subscriber.setsockopt(zmq.RCVTIMEO, timeout)
    signal.signal(signal.SIGUSR1, handle_usr1)

    while True:
        try:
            subscriber.connect(relay)

            while True:
                if time.monotonic() > message_types_last_read + 300:
                    read_message_types(eddndir)

                message = subscriber.recv()

                if not message:
                    logger.warning("subscriber.recv() returned nothing")
                    subscriber.disconnect(relay)
                    break

                signal.pthread_sigmask(signal.SIG_BLOCK, (signal.SIGUSR1,))

                process_raw_msg(eddndir, message)

                signal.pthread_sigmask(signal.SIG_UNBLOCK, (signal.SIGUSR1,))

        except zmq.ZMQError as e:
            logger.exception("ZMQ Error: %s", e)
            subscriber.disconnect(relay)

        signal.pthread_sigmask(signal.SIG_UNBLOCK, (signal.SIGUSR1,))

        time.sleep(5)


if __name__ == '__main__':
    main()
