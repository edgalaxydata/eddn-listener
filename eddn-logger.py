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
import urllib.parse
import logging
from logging.handlers import TimedRotatingFileHandler


RELAY = 'tcp://eddn.edcd.io:9500'
TIMEOUT = 60000
SCHEMABASE = 'https://eddn.edcd.io/schemas/'
EDDNDIR = '/srv/EDDN/data'
LOGPREFIX = '/srv/EDDN/log/eddn-live'
GWTS_REGEX = re.compile('^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}')


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


def restart():
    logger.warning('Restarting on USR1')
    os.execv(sys.argv[0], sys.argv)


def handle_usr1(signum, stack):
    restart()


def process_msg(eddndir, msgtype, msg, msgraw):
    global eddnfiles
    global eddnfilenames

    print(msgraw.decode('utf-8'))
    sys.stdout.flush()

    gwts = msg.get('header', {}).get('gatewayTimestamp')

    if gwts is None or not GWTS_REGEX.match(gwts):
        gwts = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

    evttype = msg.get('message', {}).get('event')

    if msgtype not in ['Journal', 'Test-Journal']:
        eventtype = msgtype
    elif evttype is not None:
        eventtype = msgtype + '.' + evttype
    else:
        eventtype = 'Unknown'

    eddnname = f'{eddndir}/{urllib.parse.quote(eventtype)}-{gwts[0:10]}.jsonl'

    eddnfile = eddnfiles.get(eventtype)

    if eddnfile is not None and eddnfilenames.get(eventtype, '') != eddnname:
        eddnfile.close()
        eddnfile = None

    if eddnfile is None:
        eddnfilenames[eventtype] = eddnname
        eddnfile = open(eddnfilenames[eventtype], 'a', encoding='utf-8')
        eddnfiles[eventtype] = eddnfile

    eddnfile.write(json.dumps(msg, sort_keys=True))
    eddnfile.write('\n')
    eddnfile.flush()


def read_message_types(eddndir):
    try:
        message_types_last_read = time.monotonic()
        with open(eddndir + '/msgtypes.txt', 'r', encoding='utf-8') as f:
            for l in f:
                schema, msgtype = l.strip().split()
                if schema.startswith(SCHEMABASE) and re.fullmatch('[A-Z][A-Za-z0-9._-]*[A-Za-z0-9]', msgtype):
                    message_types[schema] = msgtype
    except Exception:
        pass


def setup_logging(logprefix):
    handler = TimedRotatingFileHandler(
        filename=logprefix + ".log",
        when="midnight",
        interval=1,
        utc=True,
        encoding="utf-8"
    )

    handler.suffix = "%Y-%m-%d"

    formatter = logging.Formatter("[%(asctime)s][%(levelname)s][%(name)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def main():
    relay = os.environ.get('EDDN_RELAY') or RELAY
    eddndir = os.environ.get('EDDN_DIR') or EDDNDIR
    logprefix = os.environ.get('EDDN_LOGPREFIX') or LOGPREFIX

    try:
        timeout = int(os.environ.get('EDDN_TIMEOUT')) or TIMEOUT
    except:
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

                if message == False:
                    logger.warning("subscriber.recv() returned False")
                    subscriber.disconnect(relay)
                    break

                signal.pthread_sigmask(signal.SIG_BLOCK, (signal.SIGUSR1,))

                try:
                    message = zlib.decompress(message)
                except:
                    logger.exception("Unable to decompress event", extra={ 'data': data.hex() })
                    continue

                try:
                    msg = message.decode('utf-8').strip()
                except:
                    logger.exception("Unparseable event", extra={ 'data': data.decode('latin1') })
                    continue
                
                try:
                    msg = json.loads(msg)
                except:
                    logger.exception("Unparseable event: %s", msg)
                    continue

                schemaRef = msg.get('$schemaRef', '-')
                msgtype = message_types.get(schemaRef, 'Unknown')

                try:
                    process_msg(eddndir, msgtype, msg, message)
                except AttributeError:
                    logger.exception("Unhandled event: %s", json.dumps(msg))

                signal.pthread_sigmask(signal.SIG_UNBLOCK, (signal.SIGUSR1,))

        except zmq.ZMQError as e:
            logger.exception("ZMQ Error: %s", e)
            subscriber.disconnect(relay)

        signal.pthread_sigmask(signal.SIG_UNBLOCK, (signal.SIGUSR1,))

        time.sleep(5)


if __name__ == '__main__':
    main()
