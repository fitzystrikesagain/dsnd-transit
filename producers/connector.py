"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests

from utils.constants import (
    KAFKA_CONNECT_URL,
    JDBC_URL,
    POSTGRES_USER,
    POSTGRES_PASSWORD
)

logger = logging.getLogger(__name__)

CONNECTOR_NAME = "stations"


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created, skipping recreation")
        return

    logger.info("connector code not completed skipping connector creation")
    config = {
        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "batch.max.rows": "500",
        "connection.url": JDBC_URL,
        "connection.user": POSTGRES_USER,
        "connection.password": POSTGRES_PASSWORD,
        "table.whitelist": "stations",
        "mode": "incrementing",
        "incrementing.column.name": "stop_id",
        "topic.prefix": "connect-",
        "poll.interval.ms": "2000",
    }

    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": config
        }),
    )

    # Ensure a healthy response was given
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error(e)
        logger.error(resp.json()["message"])
    # else:
    print("connector created successfully")


if __name__ == "__main__":
    configure_connector()
