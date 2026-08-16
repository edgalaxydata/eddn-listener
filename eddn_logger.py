#!/usr/bin/python3

'''
EDDN logger
'''

import zlib
import json
import sys
import time
from datetime import datetime, timezone, UTC, timedelta
import os
import os.path
import re
import io
import math
import signal
import hashlib
import shutil
import tempfile
import subprocess
import traceback
import glob
from urllib.parse import quote, urlsplit, urljoin
import logging
from logging import StreamHandler
from logging.handlers import TimedRotatingFileHandler
from typing import TypedDict, Required, BinaryIO
from dataclasses import dataclass
import requests
import zmq
import paramiko
from bs4 import BeautifulSoup


RELAY = 'tcp://eddn.edcd.io:9500'
TIMEOUT = 60000
SCHEMABASE = 'https://eddn.edcd.io/schemas/'
EDDNDIR = '/srv/EDDN/data'
LOGPREFIX = '/srv/EDDN/log/eddn-live'


logger = logging.getLogger(__name__)


EDDNHeader = TypedDict('EDDNHeader', {
    'gatewayTimestamp': Required[str],
    'softwareName': Required[str],
    'softwareVersion': Required[str],
    'gameversion': str,
    'gamebuild': str,
    'uploaderID': str
}, total=False)


EDDNMessage = TypedDict('EDDNMessage', {
    '$schemaRef': Required[str],
    'header': Required[EDDNHeader],
    'message': dict
}, total=False)


@dataclass
class EDDNFile:
    '''
    Data class holding EDDN file state
    '''

    filename: str
    file: BinaryIO
    hashes: set[bytes]


# pylint: disable=too-many-instance-attributes
@dataclass
class ListenerOptions:
    '''
    Options for EDDN Listener
    '''

    relay: str
    eddndir: str
    logprefix: str
    mirror_host: str | None
    mirror_user: str | None
    mirror_dir: str
    mirror_url: str
    mirror_rsync: bool
    timeout: int


class JsonLogFormatter(logging.Formatter):
    '''
    Formatter for JSON Lines log files
    '''

    exclude_record_attrs = [
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        #"pathname",
        "filename",
        "module",
        #"exc_info",
        "exc_text",
        #"stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "taskName",
        "message",
        "asctime"
    ]

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc)

        payload = {
            'created': ts.isoformat(),
            'levelname': record.levelname,
            'name': record.name,
            'msg': record.msg,
            'args': record.args,
            'message': record.message,
            'module': record.module,
            'lineno': record.lineno,
            'funcName': record.funcName
        }

        if hasattr(record, '__dict__'):
            data = payload['data'] = {}

            for key, value in record.__dict__.items():
                if key in self.exclude_record_attrs or value is None:
                    pass
                elif key == 'exc_info':
                    exc_type, exc_value, exc_traceback = value

                    tb_lines = traceback.format_exception(
                        exc_type,
                        exc_value,
                        exc_traceback
                    )

                    data['exc_info'] = {
                        'type': exc_type.__name__ if exc_type else None,
                        'message': str(exc_value),
                        'traceback': tb_lines
                    }
                else:
                    data[key] = value

        return json.dumps(payload, default=str)


eddnfiles: dict[EDDNFile] = {}

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

signals: list[signal.Signals] = []


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


def get_eddnfile(eventtype: str, eddnname: str) -> EDDNFile:
    '''
    Open or create EDDN dump file
    '''

    eddnfile = eddnfiles.get(eventtype)

    if eddnfile is not None and eddnfile.filename != eddnname:
        eddnfile.file.close()
        eddnfile = None

    if eddnfile is None:
        file = open(  # pylint: disable=consider-using-with
            eddnname,
            'ab'
        )

        eddnfile = EDDNFile(eddnname, file, set())
        eddnfiles[eventtype] = eddnfile

    return eddnfile


def write_eddnline(eddnfile: EDDNFile, line: bytes):
    '''
    Write line to file if it hasn't already been seen
    '''

    linehash = hashlib.sha256(line).digest()

    if linehash not in eddnfile.hashes:
        eddnfile.hashes.add(linehash)

        eddnfile.file.write(line)
        eddnfile.file.write(b'\n')
        eddnfile.file.flush()

        return True

    return False


def get_eventtype(msg: EDDNMessage) -> str:
    '''
    Generate filename prefix from message schema / event type
    '''

    schema_ref = msg.get('$schemaRef', '-')
    msgtype = message_types.get(schema_ref, 'Unknown')
    evttype = msg.get('message', {}).get('event')

    if evttype is not None and not msgevent_regex.match(evttype):
        evttype = None

    if msgtype == 'Unknown':
        logger.warning(
            'Unrecognised schema %s',
            schema_ref
        )
        return 'Unknown'

    if msgtype not in ['Journal', 'Test-Journal']:
        return quote(msgtype)

    if evttype is not None:
        return quote(msgtype + '.' + evttype)

    logger.warning(
        'Unrecognised schema / event %s / %s',
        schema_ref,
        evttype
    )

    return 'Unknown'


def process_msg(eddndir: str, msg: EDDNMessage):
    '''
    Process a single EDDN message
    '''

    eventtype = get_eventtype(msg)

    gwts = msg.get('header', {}).get('gatewayTimestamp')

    if gwts is None or not gwts_regex.match(gwts):
        logger.warning('Unrecognised gateway timestamp %s', gwts)
        gwts = datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S')

    eddnname = f'{eventtype}-{gwts[0:10]}.jsonl'
    eddnname = os.path.join(eddndir, eddnname)

    eddnfile = get_eddnfile(eventtype, eddnname)

    line = json.dumps(msg, sort_keys=True).encode('utf-8')

    write_eddnline(eddnfile, line)


def process_raw_msg(eddndir, message):
    '''
    Process a raw message from EDDN relay
    '''

    try:
        message = zlib.decompress(message).strip(b' \t\n\r\x0b\f\0')
    except (zlib.error, TypeError):
        logger.exception(
            "Unable to decompress event",
            extra={'data': message.hex()}
        )

        return

    try:
        msg = message.decode('utf-8')
    except UnicodeDecodeError:
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


def is_valid_line(line: bytes,
                  eddnname: str,
                  pos: int,
                  linenum: int) -> bool:
    '''
    Validate EDDN dump line
    '''

    if b'\0' in line:
        logger.error(
            "NUL bytes in file %s at %d (line %d col %d)",
            eddnname,
            pos + line.index(b'\0'),
            linenum,
            line.index(b'\0')
        )

        return False

    try:
        msg = line.decode('utf-8')
    except UnicodeDecodeError as e:
        logger.exception(
            "Unparseable event in file %s at %d (line %d col %d)",
            eddnname,
            pos + e.start,
            linenum,
            e.start,
            extra={'data': line}
        )

        return False

    try:
        _ = json.loads(msg)
    except json.JSONDecodeError as e:
        logger.exception(
            "Unparseable event in file %s at %d (line %d col %d)",
            eddnname,
            pos + e.pos,
            linenum,
            e.pos,
            extra={'data': line}
        )

        return False

    return True


def init_local_file(eddnfile: EDDNFile, eddnname: str):
    '''
    Initialise state from existing EDDN dump file
    '''

    with open(eddnname, 'rb+') as file:
        with io.BufferedReader(file, 1048576) as buf:
            pos = 0
            linenum = 1

            while line := buf.readline():
                if not is_valid_line(line, eddnname, pos, linenum):
                    with open(eddnname + ".bad", 'wb') as ofile:
                        shutil.copyfileobj(file, ofile)

                    file.truncate(pos)
                    break

                pos += len(line)

                line = line.strip()

                linehash = hashlib.sha256(line).digest()
                eddnfile.hashes.add(linehash)

                if (linenum % 1000) == 0:
                    logger.info(
                        'Loaded %d lines from file %s',
                        linenum,
                        eddnname
                    )

                linenum += 1

            logger.info(
                'Loaded %d lines from file %s',
                linenum,
                eddnname
            )


def init_local_files(eddndir: str, suffix: str):
    '''
    Initialise state from existing EDDN dump files
    '''

    for filename in os.listdir(eddndir):
        if filename.endswith(suffix):
            eddnname = os.path.join(eddndir, filename)
            eventtype = filename[:-len(suffix)]
            logger.info('Loading events from local file %s', eddnname)

            eddnfile = get_eddnfile(eventtype, eddnname)

            init_local_file(eddnfile, eddnname)


def sync_mirror_file(eddnfile: EDDNFile, remote_name: str, file: BinaryIO):
    '''
    Sync EDDN events from another stream
    '''

    logger.info(
        'Loading events from file %s',
        remote_name
    )

    pos = 0
    linenum = 1
    copied_lines = 0

    for line in file:
        line_len = len(line)
        line = line.strip(b' \t\n\r\x0b\f\0')

        if not is_valid_line(line, remote_name, pos, linenum):
            continue

        if write_eddnline(eddnfile, line):
            copied_lines += 1

        if (linenum % 1000) == 0:
            logger.info(
                'Loaded %d lines from file %s (%d copied)',
                linenum,
                remote_name,
                copied_lines
            )

        pos += line_len
        linenum += 1

    logger.info(
        'Loaded %d lines from file %s (%d copied)',
        linenum,
        remote_name,
        copied_lines
    )


def sync_mirror_rsync(mirror_host: str,
                      mirror_user: str,
                      mirror_dir: str,
                      eddndir: str,
                      suffix: str):
    '''
    Sync from mirror using a tempdir
    '''

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            command = [
                '/usr/bin/rsync',
                '-vz',
                f'{mirror_user}@{mirror_host}:{mirror_dir}/*{suffix}',
                tmpdir,
                f'--copy-dest={eddndir}'
            ]

            logger.info(
                'Syncing files from mirror %s',
                mirror_host
            )

            subprocess.run(
                command,
                check=True,
                stdin=subprocess.DEVNULL
            )
        except subprocess.SubprocessError:
            logger.exception(
                'Failed to sync from mirror %s',
                mirror_host
            )
            return

        for filename in os.listdir(tmpdir):
            tmpname = os.path.join(tmpdir, filename)
            remote_name = os.path.join(mirror_dir, filename)
            eddnname = os.path.join(eddndir, filename)
            eventtype = filename[:-len(suffix)]

            eddnfile = get_eddnfile(eventtype, eddnname)

            with open(tmpname, 'rb') as file:
                with io.BufferedReader(file, 1048576) as buf:
                    sync_mirror_file(eddnfile, f'remote:{remote_name}', buf)


def sync_mirror_sftp(mirror_host: str,
                     mirror_user: str,
                     mirror_dir: str,
                     eddndir: str,
                     suffix: str):
    '''
    Sync from mirror using SFTP
    '''

    try:
        logger.info('Connecting to SFTP host %s', mirror_host)
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()

        ssh_client.connect(
            mirror_host,
            username=mirror_user,
            timeout=5,
            allow_agent=False,
            banner_timeout=5,
            auth_timeout=5,
            channel_timeout=5
        )

        sftp = ssh_client.open_sftp()
    except (OSError, paramiko.SSHException):
        logger.exception("Error connecting to mirror host")
        return

    logger.info('Enumerating files on SFTP host %s', mirror_host)

    for filename in sftp.listdir(mirror_dir):
        if filename.endswith(suffix):
            remote_name = os.path.join(mirror_dir, filename)
            eddnname = os.path.join(eddndir, filename)
            eventtype = filename[:-len(suffix)]

            eddnfile = get_eddnfile(eventtype, eddnname)

            with sftp.open(remote_name, 'rb') as file:
                with io.BufferedReader(file, 1048576) as buf:
                    sync_mirror_file(eddnfile, f'remote:{remote_name}', buf)


def https_listdir(mirror_url: str,
                  session: requests.Session,
                  suffix: str):
    '''
    Retrieve files from HTTPS directory listing
    '''

    try:
        response = session.get(mirror_url, timeout=5)
        response.raise_for_status()
        html = response.text
        mirror_url = response.url
    except OSError:
        logger.exception("Error listing HTTPS url")
        return {}

    soup = BeautifulSoup(html, "html.parser")

    base = urlsplit(mirror_url)
    base_path = base.path

    if not base_path.endswith('/'):
        base_path += '/'

    files = {}

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()

        if not href:
            continue

        link_url = urljoin(mirror_url, href)
        link = urlsplit(link_url)

        if link.scheme != base.scheme or \
            link.netloc != base.netloc or \
            not link.path.startswith(base_path) or \
            not link.path.endswith(suffix):
            continue

        relpath = link.path[len(base_path):]
        msgtype = relpath[:len(suffix)]

        if '/' in relpath or \
           '\\' in relpath or \
           not eventtype_regex.match(msgtype):
            continue

        files[relpath] = link_url

    return files


def sync_mirror_https_file(url: str,
                           filename: str,
                           eddndir: str,
                           suffix: str):
    '''
    Sync file from mirror using HTTPS
    '''

    eddnname = os.path.join(eddndir, filename)
    eventtype = filename[:-len(suffix)]

    eddnfile = get_eddnfile(eventtype, eddnname)

    with requests.get(url, stream=True, timeout=5) as response:
        with io.BufferedReader(response.raw, 1048576) as buf:
            sync_mirror_file(eddnfile, url, buf)


def sync_mirror_https(mirror_url: str,
                      eddndir: str,
                      suffix: str) -> dict[str, str]:
    '''
    Sync from mirror using HTTPS
    '''

    with requests.Session() as session:
        files = https_listdir(mirror_url, session, suffix)

        for name, url in files.items():
            sync_mirror_https_file(url, name, eddndir, suffix)


def sync_mirror_url(mirror_url: str,
                    eddndir: str,
                    suffix: str):
    '''
    Sync from mirror using a URL
    '''

    parsed = urlsplit(mirror_url)

    match parsed.scheme:
        case 'rsync+ssh':
            sync_mirror_rsync(
                parsed.hostname,
                parsed.username,
                parsed.path,
                eddndir,
                suffix
            )
        case 'sftp':
            sync_mirror_sftp(
                parsed.hostname,
                parsed.username,
                parsed.path,
                eddndir,
                suffix
            )
        case 'https':
            sync_mirror_https(
                mirror_url,
                eddndir,
                suffix
            )


def sync_mirror(opts: ListenerOptions):
    '''
    Sync EDDN events from another host
    '''

    today = datetime.now(UTC)
    yesterday = today - timedelta(days=1)
    today = today.strftime('%Y-%m-%d')
    yesterday = yesterday.strftime('%Y-%m-%d')

    suffixes = [
        f'-{today}.jsonl'
    ]

    if not glob.glob(f"*-{today}.jsonl", root_dir=opts.eddndir):
        suffixes = [
            f'-{yesterday}.jsonl',
            f'-{today}.jsonl'
        ]

    for suffix in suffixes:
        if init_local_files(opts.eddndir, suffix):
            if opts.mirror_url is not None:
                sync_mirror_url(
                    opts.mirror_url,
                    opts.eddndir,
                    suffix
                )
            elif opts.mirror_host is not None and \
                 opts.mirror_user is not None:
                if opts.mirror_rsync:
                    sync_mirror_rsync(
                        opts.mirror_host,
                        opts.mirror_user,
                        opts.mirror_dir,
                        opts.eddndir,
                        suffix
                    )
                else:
                    sync_mirror_sftp(
                        opts.mirror_host,
                        opts.mirror_user,
                        opts.mirror_dir,
                        opts.eddndir,
                        suffix
                    )


def listen(subscriber: zmq.Socket, timeout: int, eddndir: str):
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

            if hasattr(signal, 'SIG_BLOCK') and signals:
                signal.pthread_sigmask(signal.SIG_BLOCK, signals)

            process_raw_msg(eddndir, message)

            if hasattr(signal, 'SIG_UNBLOCK') and signals:
                signal.pthread_sigmask(signal.SIG_UNBLOCK, signals)
        elif now_msec - last_recv_msec > timeout:
            logger.error('No messages received')
            return


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

    json_handler = TimedRotatingFileHandler(
        filename=logprefix + ".jsonl",
        when="midnight",
        interval=1,
        utc=True,
        encoding="utf-8"
    )

    json_handler.suffix = "%Y-%m-%d"

    stderr_handler = StreamHandler(sys.stderr)

    formatter = logging.Formatter(
        "[%(asctime)s][%(levelname)s][%(name)s] %(message)s"
    )

    json_formatter = JsonLogFormatter()

    file_handler.setFormatter(formatter)
    json_handler.setFormatter(json_formatter)
    stderr_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(json_handler)
    logger.addHandler(stderr_handler)

    logger.setLevel(logging.INFO)


def setup_signals():
    '''
    Setup available signals
    '''

    if hasattr(signal, 'SIGUSR1'):
        signals.append(signal.SIGUSR1)
        signal.signal(signal.SIGUSR1, handle_usr1)
    if hasattr(signal, 'SIGINT'):
        signals.append(signal.SIGINT)


def get_options() -> ListenerOptions:
    '''
    Process options from environment
    '''

    relay = os.environ.get('EDDN_RELAY') or RELAY
    eddndir = os.environ.get('EDDN_DIR') or EDDNDIR
    logprefix = os.environ.get('EDDN_LOGPREFIX') or LOGPREFIX
    mirror_host = os.environ.get('EDDN_MIRROR_HOST')
    mirror_user = os.environ.get('EDDN_MIRROR_USER')
    mirror_url = os.environ.get('EDDN_MIRROR_URL')
    mirror_dir = os.environ.get('EDDN_MIRROR_DIR') or eddndir
    mirror_rsync = os.environ.get('EDDN_MIRROR_RSYNC', 'False')
    mirror_rsync = mirror_rsync.lower() in ('true', 'yes', '1')

    try:
        timeout = int(os.environ.get('EDDN_TIMEOUT')) or TIMEOUT
    except (ValueError, TypeError):
        timeout = TIMEOUT

    return ListenerOptions(
        relay=relay,
        eddndir=eddndir,
        logprefix=logprefix,
        mirror_host=mirror_host,
        mirror_user=mirror_user,
        mirror_dir=mirror_dir,
        mirror_url=mirror_url,
        mirror_rsync=mirror_rsync,
        timeout=timeout
    )


def main_loop(subscriber: zmq.Socket, opts: ListenerOptions):
    '''
    Main listener loop body
    '''

    try:
        logger.info('Connecting to relay %s', opts.relay)
        subscriber.connect(opts.relay)

        sync_mirror(opts)

        listen(subscriber, opts.timeout, opts.eddndir)

    except zmq.ZMQError as e:
        logger.exception("ZMQ Error: %s", e)
    finally:
        subscriber.disconnect(opts.relay)

        if hasattr(signal, 'SIG_UNBLOCK') and signals:
            signal.pthread_sigmask(signal.SIG_UNBLOCK, signals)

    time.sleep(5)


def main():
    '''
    Main function
    '''

    opts = get_options()

    setup_logging(opts.logprefix)
    setup_signals()

    context = zmq.Context.instance()
    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.SUBSCRIBE, b'')
    subscriber.setsockopt(zmq.RCVTIMEO, opts.timeout)
    subscriber.set(zmq.RCVHWM, 10000)

    try:
        while True:
            main_loop(subscriber, opts)
    except KeyboardInterrupt:
        logger.warning("Exiting")


if __name__ == '__main__':
    main()
