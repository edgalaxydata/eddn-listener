#!/usr/bin/python3

'''
EDDN logger
'''

import zlib
import json
import sys
import time
from datetime import datetime
import os
import os.path
import re
import math
import signal
from urllib.parse import quote
import logging
from logging import StreamHandler
from logging.handlers import TimedRotatingFileHandler
import zmq


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
    '''
    Restart process on USR1 signal
    '''

    logger.warning('Restarting on USR1')
    os.execv(sys.argv[0], sys.argv)


def handle_usr1():
    '''
    Handle USR1 signal
    '''

    restart()


def process_msg(eddndir, msg):
    '''
    Process a single EDDN message
    '''

    schema_ref = msg.get('$schemaRef', '-')
    msgtype = message_types.get(schema_ref, 'Unknown')

    gwts = msg.get('header', {}).get('gatewayTimestamp')

    if gwts is None or not gwts_regex.match(gwts):
        logger.warning('Unrecognised gateway timestamp %s', gwts)
        gwts = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

    evttype = msg.get('message', {}).get('event')

    if evttype is not None and not msgevent_regex.match(evttype):
        evttype = None

    if msgtype == 'Unknown':
        logger.warning(
            'Unrecognised schema %s',
            schema_ref
        )
        eventtype = 'Unknown'
    elif msgtype not in ['Journal', 'Test-Journal']:
        eventtype = quote(msgtype)
    elif evttype is not None:
        eventtype = quote(msgtype + '.' + evttype)
    else:
        logger.warning(
            'Unrecognised schema / event %s / %s',
            schema_ref,
            evttype
        )
        eventtype = 'Unknown'

    eddnname = f'{eddndir}/{eventtype}-{gwts[0:10]}.jsonl'

    eddnfile = eddnfiles.get(eventtype)
    prevname = eddnfilenames.get(eventtype, '')

    if eddnfile is not None and prevname != eddnname:
        eddnfile.close()
        eddnfile = None

    if eddnfile is None:
        eddnfilenames[eventtype] = eddnname

        eddnfile = open(  # pylint: disable=consider-using-with
            eddnfilenames[eventtype],
            'a',
            encoding='utf-8'
        )

        eddnfiles[eventtype] = eddnfile

    eddnfile.write(json.dumps(msg, sort_keys=True))
    eddnfile.write('\n')
    eddnfile.flush()


def process_raw_msg(eddndir, message):
    '''
    Process a raw message from EDDN relay
    '''

    try:
        message = zlib.decompress(message)
    except (zlib.error, TypeError):
        logger.exception(
            "Unable to decompress event",
            extra={'data': message.hex()}
        )

        return

    try:
        msg = message.decode('utf-8').strip()
    except UnicodeError:
        logger.exception(
            "Unparseable event",
            extra={'data': message.decode('latin1')}
        )
        return

    try:
        msg = json.loads(msg)
    except json.JSONDecodeError:
        logger.exception("Unparseable event: %s", msg)
        return

    try:
        process_msg(eddndir, msg)
    except AttributeError:
        logger.exception("Unhandled event: %s", json.dumps(msg))


def update_message_type(line):
    '''
    Update message type from message types file
    '''

    try:
        schema, msgtype = line.strip().split()
        if schema.startswith(SCHEMABASE) and eventtype_regex.match(msgtype):
            message_types[schema] = msgtype
    except ValueError:
        pass


def read_message_types(eddndir):
    '''
    Update message types from message types file
    '''

    try:
        with open(eddndir + '/msgtypes.txt', 'r', encoding='utf-8') as file:
            for line in file:
                update_message_type(line)
    except (IOError, UnicodeError):
        pass


def setup_logging(logprefix):
    '''
    Setup logging
    '''

    file_handler = TimedRotatingFileHandler(
        filename=logprefix + ".log",
        when="midnight",
        interval=1,
        utc=True,
        encoding="utf-8"
    )

    file_handler.suffix = "%Y-%m-%d"

    stderr_handler = StreamHandler(sys.stderr)

    formatter = logging.Formatter(
        "[%(asctime)s][%(levelname)s][%(name)s] %(message)s"
    )

    file_handler.setFormatter(formatter)
    stderr_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stderr_handler)

    logger.setLevel(logging.INFO)


def listen(subscriber, timeout, eddndir):
    '''
    Listen to ZMQ subscriber socket
    '''

    last_recv_msec = time.monotonic_ns() // 1000000
    last_stat_msec = last_recv_msec
    line_count = 0
    message_types_last_read = -math.inf

    while True:
        if time.monotonic() > message_types_last_read + 300:
            logger.info('Reading message types')
            message_types_last_read = time.monotonic()
            read_message_types(eddndir)

        now_msec = time.monotonic_ns() // 1000000

        while now_msec - last_stat_msec > 5000:
            if line_count > 0:
                logger.info("Received %d events", line_count)
                line_count = 0
            last_stat_msec += 5000

        if subscriber.poll(last_stat_msec + 5000 - now_msec):
            last_recv_msec = now_msec
            line_count += 1
            message = subscriber.recv()

            if not message:
                logger.warning("subscriber.recv() returned nothing")
                return

            if hasattr(signal, 'SIGUSR1') and hasattr(signal, 'SIG_BLOCK'):
                signal.pthread_sigmask(signal.SIG_BLOCK, (signal.SIGUSR1,))

            process_raw_msg(eddndir, message)

            if hasattr(signal, 'SIGUSR1') and hasattr(signal, 'SIG_UNBLOCK'):
                signal.pthread_sigmask(signal.SIG_UNBLOCK, (signal.SIGUSR1,))
        elif now_msec - last_recv_msec > timeout:
            logger.error('No messages received')
            return


def main():
    '''
    Main function
    '''

    relay = os.environ.get('EDDN_RELAY') or RELAY
    eddndir = os.environ.get('EDDN_DIR') or EDDNDIR
    logprefix = os.environ.get('EDDN_LOGPREFIX') or LOGPREFIX

    try:
        timeout = int(os.environ.get('EDDN_TIMEOUT')) or TIMEOUT
    except (ValueError, TypeError):
        timeout = TIMEOUT

    setup_logging(logprefix)

    context = zmq.Context.instance()
    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.SUBSCRIBE, b'')
    subscriber.setsockopt(zmq.RCVTIMEO, timeout)

    if hasattr(signal, 'SIGUSR1'):
        signal.signal(signal.SIGUSR1, handle_usr1)

    while True:
        try:
            subscriber.connect(relay)

            listen(subscriber, timeout, eddndir)

        except zmq.ZMQError as e:
            logger.exception("ZMQ Error: %s", e)

        subscriber.disconnect(relay)

        if hasattr(signal, 'SIGUSR1') and hasattr(signal, 'SIG_UNBLOCK'):
            signal.pthread_sigmask(signal.SIG_UNBLOCK, (signal.SIGUSR1,))

        time.sleep(5)


if __name__ == '__main__':
    main()
