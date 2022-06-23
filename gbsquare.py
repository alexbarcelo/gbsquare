#!/usr/bin/env python

from distutils.util import strtobool
import json
import logging
import os
import sqlite3
from tempfile import NamedTemporaryFile
from time import sleep
from influxdb import InfluxDBClient

from webdav3.client import Client


# Environment variables that make WebDAV work
WEBDAV_HOSTNAME = os.environ["WEBDAV_HOSTNAME"]
WEBDAV_LOGIN = os.environ["WEBDAV_LOGIN"]
WEBDAV_PASSWORD = os.environ["WEBDAV_PASSWORD"]
WEBDAV_PATH = os.environ["WEBDAV_PATH"]

# The ones that make InfluxDB work
INFLUXDB_HOST = os.environ["INFLUXDB_HOST"]
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT", "8086"))
INFLUXDB_USERNAME = os.getenv("INFLUXDB_USERNAME")
INFLUXDB_PASSWORD = os.getenv("INFLUXDB_PASSWORD")
INFLUXDB_SSL = strtobool(os.getenv("INFLUXDB_SSL", "False"))
INFLUXDB_VERIFY_SSL = strtobool(os.getenv("INFLUXDB_VERIFY_SSL", "False"))
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE", "miband")

# The user can provide a JSON for extra tags
EXTRA_TAGS = json.loads(os.getenv("EXTRA_TAGS", "{}"))

# By default, poll every 10 minutes (etag checking)
POLLING_SLEEP = os.getenv("POLLING_SLEEP", 600)

# By default, each poll syncs last week of 
ENTRIES_TO_SYNC = int(os.getenv("ENTRIES_TO_SYNC", 10080))

# To debug or not to debug
DEBUG = strtobool(os.getenv("DEBUG", "False"))

ETAG_DB = None

logger = logging.getLogger('gbsquare')


def connect_to_influxdb():
    options = {
        "host": INFLUXDB_HOST,
        "port": INFLUXDB_PORT,
        "ssl": INFLUXDB_SSL,
        "verify_ssl": INFLUXDB_VERIFY_SSL
    }

    if INFLUXDB_USERNAME and INFLUXDB_PASSWORD:
        options["username"] = INFLUXDB_USERNAME
        options["password"] = INFLUXDB_PASSWORD

    client = InfluxDBClient(**options)
    client.switch_database(INFLUXDB_DATABASE)

    return client


def process_db(db_file, limit):
    con = sqlite3.connect(db_file)
    cur = con.cursor()

    client = connect_to_influxdb()

    raw_datapoints = cur.execute('SELECT TIMESTAMP, RAW_INTENSITY, STEPS, RAW_KIND, HEART_RATE FROM `MI_BAND_ACTIVITY_SAMPLE` ORDER BY `TIMESTAMP` DESC LIMIT %d' % limit)
    influx_points = list()

    tags = EXTRA_TAGS
    tags["origin"] = "gbsquare"

    for timestamp, raw_intensity, steps, raw_kind, heart_rate in raw_datapoints:
        p = {
            "measurement": "miBandActivitySample",
            "tags": tags,
            "time": timestamp,
            "fields": {
                "raw_intensity": raw_intensity,
                "steps": steps,
                "raw_kind": raw_kind,
                "heart_rate": heart_rate,
            }
        }

        influx_points.append(p)

    client.write_points(influx_points, time_precision="s")

    con.close()

def check_and_process_db(webdav_client, force_sync=False):
    global ETAG_DB

    new_etag = webdav_client.info(WEBDAV_PATH)["etag"]

    if not force_sync:
        logger.debug("Consulted %s on WebDAV. Received etag: %s", WEBDAV_PATH, new_etag)
        if new_etag == ETAG_DB:
            # Nothing to do
            logger.debug("No changes detected, skipping update")
            return

    ETAG_DB = new_etag

    logger.info("Proceeding to download new version of GadgetBridge database")
    with NamedTemporaryFile() as file:
        webdav_client.download(remote_path=WEBDAV_PATH, local_path=file.name)
        process_db(file.name, limit=-1 if force_sync else ENTRIES_TO_SYNC)


def main():
    logger.info("Initializing GBSquare application")
    global ETAG_DB

    logger.debug("WebDAV configuration:\n"
                 "\tWEBDAV_HOSTNAME: %s\n"
                 "\tWEBDAV_LOGIN: %s\n"
                 "\tWEBDAV_PASSWORD: %s\n", 
                 WEBDAV_HOSTNAME, WEBDAV_LOGIN, "*" * len(WEBDAV_PASSWORD))

    logger.info("Extra tags that I will be using: %s", EXTRA_TAGS)

    webdav_client = Client({
        "webdav_hostname": WEBDAV_HOSTNAME,
        "webdav_login": WEBDAV_LOGIN,
        "webdav_password": WEBDAV_PASSWORD,
    })

    # Note that the check will always evaluate as True on startup
    check_and_process_db(webdav_client, force_sync=True)

    logger.info("Starting application loop")
    while True:
        sleep(POLLING_SLEEP)
        logger.debug("Waking up from sleep, proceeding to poll WebDAV")
        check_and_process_db(webdav_client, force_sync=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG if DEBUG else logging.INFO)
    main()
