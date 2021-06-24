import os

"""
Container names
"""
ZOOKEEPER_CONTAINER = os.environ.get("ZOOKEEPER_CONTAINER")
BROKER_CONTAINER = os.environ.get("BROKER_CONTAINER")
SCHEMA_REGISTRY_CONTAINER = os.environ.get("SCHEMA_REGISTRY_CONTAINER")
REST_PROXY_CONTAINER = os.environ.get("REST_PROXY_CONTAINER")
CONNECT_CONTAINER = os.environ.get("CONNECT_CONTAINER")
KSQL_CONTAINER = os.environ.get("KSQL_CONTAINER")
CONNECT_UI_CONTAINER = os.environ.get("CONNECT_UI_CONTAINER")
TOPICS_UI_CONTAINER = os.environ.get("TOPICS_UI_CONTAINER")
SCHEMA_REGISTRY_UI_CONTAINER = os.environ.get("SCHEMA_REGISTRY_UI_CONTAINER")
POSTGRES_CONTAINER = os.environ.get("POSTGRES_CONTAINER")

"""
Service URLs
"""
BROKER_URL = "PLAINTEXT://BROKER_CONTAINER:9092"
SCHEMA_REGISTRY_URL = "http://SCHEMA_REGISTRY_CONTAINER:8081"
KAFKA_CONNECT_URL = "http://CONNECT_CONTAINER:8083/connectors"
REST_PROXY_URL = "http://SCHEMA_REGISTRY_CONTAINER:8082"

"""
Postgres config
"""
POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
POSTGRES_DB = os.environ.get("POSTGRES_DB")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}"

"""
Kafka constants
"""
CTA_PREFIX = "org.cta"

"""
Headers
"""


def get_content_type_headers(serde_format):
    """
    Returns header value for the chosen SerDe format.
    """
    return {
        "avro": "application/vnd.kafka.avro.v2+json",
        "binary": "application/vnd.kafka.binary.v2+json",
        "json": "application/vnd.kafka.json.v2+json",
    }[serde_format]
