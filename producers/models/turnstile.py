"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from producers.models import Producer
from producers.models import TurnstileHardware
from utils.constants import CTA_PREFIX

logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    value_schema = avro.load(
       f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
                        .replace("/", "_and_")
                        .replace(" ", "_")
                        .replace("-", "_")
                        .replace("'", "")
        )

        super().__init__(
            f"{CTA_PREFIX}.turnstile.{station_name}",
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=2,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        logger.info("turnstile kafka integration incomplete - skipping")
        for _ in range(num_entries):
            try:
                self.producer.produce(
                    topic=self.topic_name,
                    key={"timestamp": self.time_millis()},
                    value={
                        "station_id": self.station.station_id,
                        "station_name": self.station.station_name,
                        "line": self.station.line,
                    },
                )
            except Exception as e:
                logger.error(e)
