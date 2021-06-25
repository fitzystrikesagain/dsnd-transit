import os
# todo: clean this up, a lot
"""
Container names
"""
ZOOKEEPER_CONTAINER = os.environ.get("ZOOKEEPER_CONTAINER", "localhost")
BROKER_CONTAINER = os.environ.get("BROKER_CONTAINER", "localhost")
SCHEMA_REGISTRY_CONTAINER = os.environ.get("SCHEMA_REGISTRY_CONTAINER",
                                           "localhost")
REST_PROXY_CONTAINER = os.environ.get("REST_PROXY_CONTAINER", "localhost")
CONNECT_CONTAINER = os.environ.get("CONNECT_CONTAINER", "localhost")
KSQL_CONTAINER = os.environ.get("KSQL_CONTAINER", "localhost")
CONNECT_UI_CONTAINER = os.environ.get("CONNECT_UI_CONTAINER", "localhost")
TOPICS_UI_CONTAINER = os.environ.get("TOPICS_UI_CONTAINER", "localhost")
SCHEMA_REGISTRY_UI_CONTAINER = os.environ.get(
    "SCHEMA_REGISTRY_UI_CONTAINER", "localhost")
POSTGRES_CONTAINER = os.environ.get("POSTGRES_CONTAINER", "localhost")

"""
Service URLs
"""
BROKER_URL = "PLAINTEXT://BROKER_CONTAINER:9092"
SCHEMA_REGISTRY_URL = f"http://{SCHEMA_REGISTRY_CONTAINER}:8081"
KAFKA_CONNECT_URL = f"http://{CONNECT_CONTAINER}:8083/connectors"
REST_PROXY_URL = f"http://{SCHEMA_REGISTRY_CONTAINER}:8082"

"""
Postgres config
"""
POSTGRES_USER = "cta_admin"
POSTGRES_PASSWORD = "chicago"
POSTGRES_DB = "cta"
POSTGRES_HOST = "postgres"
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
