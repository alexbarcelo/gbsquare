#!/usr/bin/env python

from distutils.util import strtobool
from datetime import datetime
import json
import logging
import os
import sqlite3
from tempfile import NamedTemporaryFile
from time import sleep
from influxdb import InfluxDBClient

import requests
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

# The username, used for tagging data on influxdb as well as triggering webhooks on update
USER_TAG = os.getenv("USER_TAG")
# The user can provide a JSON for extra tags
EXTRA_TAGS = json.loads(os.getenv("EXTRA_TAGS", "{}"))
# Include the user tag, that we will be using anyways
if USER_TAG:
    EXTRA_TAGS["user"] = USER_TAG

# A Webhook that will be called after every update / sync
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# By default, poll every 10 minutes (etag checking)
POLLING_SLEEP = os.getenv("POLLING_SLEEP", 600)

# By default, each poll syncs last week of 
ENTRIES_TO_SYNC = int(os.getenv("ENTRIES_TO_SYNC", 10080))

# To debug or not to debug
DEBUG = strtobool(os.getenv("DEBUG", "False"))

MAX_POINTS_PER_REQUEST = 20000
ETAG_DB = None

logger = logging.getLogger('gbsquare')


def connect_to_influxdb() -> InfluxDBClient:
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


def process_device(device_id: int, tags: dict, con: sqlite3.Connection, influx_client: InfluxDBClient, starting_timestamp: int):
    cur = con.cursor()

    influx_points = list()
    i = 0

    raw_datapoints = cur.execute(f"""
        SELECT TIMESTAMP, RAW_INTENSITY, STEPS, RAW_KIND, HEART_RATE 
        FROM `MI_BAND_ACTIVITY_SAMPLE`
        WHERE `DEVICE_ID`="{device_id}" 
            AND `TIMESTAMP` > {starting_timestamp}
        ORDER BY `TIMESTAMP` DESC
    """)

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

        i += 1
        if i > MAX_POINTS_PER_REQUEST:
            logger.debug("Reached MAX_POINTS_PER_REQUEST (#%d), writing to InfluxDB", MAX_POINTS_PER_REQUEST)
            influx_client.write_points(influx_points, time_precision="s")
            influx_points.clear()
            i = 0

    influx_client.write_points(influx_points, time_precision="s")
    cur.close()


def process_db(db_file: str, force_sync: bool):
    con = sqlite3.connect(db_file)
    cur = con.cursor()

    client = connect_to_influxdb()
    devices = cur.execute('SELECT _id FROM `DEVICE`')

    for device, in devices:
        device_id = int(device)
        logger.debug("Processing device %d", device_id)

        tags = EXTRA_TAGS.copy()
        tags["origin"] = "gbsquare"
        tags["device_id"] = device_id

        if force_sync:
            ts = 0
            logger.info("Retrieving info for device %d, forcing full sync", device_id)
        else:
            last_influx_entry = client.query(
                f"""
                SELECT heart_rate
                FROM miband.autogen.miBandActivitySample
                WHERE "user"='{USER_TAG}' 
                    AND "device_id"='{device_id}'
                ORDER BY time DESC 
                LIMIT 1
            """)
            last_time = list(last_influx_entry.get_points())[0]["time"]
            dt = datetime.fromisoformat(last_time)
            ts = dt.timestamp()

            logger.info("Retrieving info for device %d since %s", device_id, ts)

        process_device(device_id, tags, con, client, ts)
        
    con.close()


def check_and_process_db(webdav_client: Client, force_sync=False) -> bool:
    global ETAG_DB

    new_etag = webdav_client.info(WEBDAV_PATH)["etag"]

    if not force_sync:
        logger.debug("Consulted %s on WebDAV. Received etag: %s", WEBDAV_PATH, new_etag)
        if new_etag == ETAG_DB:
            # Nothing to do
            logger.debug("No changes detected, skipping update")
            return False

    ETAG_DB = new_etag

    logger.info("Proceeding to download new version of GadgetBridge database")
    with NamedTemporaryFile() as file:
        webdav_client.download(remote_path=WEBDAV_PATH, local_path=file.name)
        process_db(file.name, force_sync)

    return True


def trigger_webhook():
    if not WEBHOOK_URL:
        logger.info("No webhook URL configured, not calling anywhere")
        return

    logger.debug("Proceeding to webhook to %s", WEBHOOK_URL)
    r = requests.post(WEBHOOK_URL, json={"user": USER_TAG})
    if r.status_code == 200:
        logger.info("Webhook triggered and successful")
    else:
        logger.warning("Error when calling %s. Status code: %d", WEBHOOK_URL, r.status_code)


def main():
    logger.info("Initializing GBSquare application")
    global ETAG_DB

    logger.debug("WebDAV configuration:\n"
                 "\tWEBDAV_HOSTNAME: %s\n"
                 "\tWEBDAV_LOGIN: %s\n"
                 "\tWEBDAV_PASSWORD: %s\n", 
                 WEBDAV_HOSTNAME, WEBDAV_LOGIN, "*" * len(WEBDAV_PASSWORD))

    if WEBHOOK_URL:
        logger.info("Webhook has been configured to the following URL: %s", WEBHOOK_URL)
    else:
        logger.info("No webhook has been configured")

    logger.info("Extra tags that I will be using: %s", EXTRA_TAGS)

    webdav_client = Client({
        "webdav_hostname": WEBDAV_HOSTNAME,
        "webdav_login": WEBDAV_LOGIN,
        "webdav_password": WEBDAV_PASSWORD,
    })

    # Note that the check will always evaluate as True on startup
    check_and_process_db(webdav_client, force_sync=True)

    trigger_webhook()

    logger.info("Starting application loop")
    while True:
        sleep(POLLING_SLEEP)
        logger.debug("Waking up from sleep, proceeding to poll WebDAV")
        changed = check_and_process_db(webdav_client, force_sync=False)

        if changed:
            trigger_webhook()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG if DEBUG else logging.INFO)
    main()
