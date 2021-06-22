"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

from utils.constants import BROKER_URL, SCHEMA_REGISTRY_URL

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "broker_url": BROKER_URL,
            "schema_registry_url": SCHEMA_REGISTRY_URL,
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer({
            "bootstrap.servers": BROKER_URL},
            schema_registry=SCHEMA_REGISTRY_URL,
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = AdminClient({'bootstrap.servers': BROKER_URL})
        topic = NewTopic(
            self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.num_replicas
        )
        try:
            client.create_topics(topic)
            logger.info(
                "topic creation kafka integration incomplete - skipping")
        except Exception as e:
            logger.error(e)

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        try:
            self.producer.flush(5)
            self.producer.close()
            logger.info("producer close complete")
        except Exception as e:
            logger.error(e)
            logger.info("producer close incomplete - skipping")
