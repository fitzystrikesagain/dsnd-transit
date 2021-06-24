"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse

import requests

from producers.models import Producer
from utils.constants import REST_PROXY_URL
from utils.constants import get_content_type_headers

logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    key_schema = None
    value_schema = None

    winter_months = {0, 1, 2, 3, 10, 11}
    summer_months = {6, 7, 8}

    def __init__(self, month):
        super().__init__(
            "com.cta.weather.value",
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            num_partitions=3,
            num_replicas=1
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_schema is None:
            outfile = f"{Path(__file__).parents[0]}/schemas/weather_key.json"
            with open(outfile)as f:
                Weather.key_schema = json.load(f)

        # TODO: Define this value schema in `schemas/weather_value.json
        outfile = f"{Path(__file__).parents[0]}/schemas/weather_value.json"
        if Weather.value_schema is None:
            with open(outfile) as f:
                Weather.value_schema = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)),
                         100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        resp = requests.post(
            REST_PROXY_URL,
            headers={"Content-Type": get_content_type_headers("avro")},
            data=json.dumps({
                "key_schema": json.dumps(Weather.key_schema),
                "value_schema": json.dumps(Weather.value_schema),
                "records": [
                    {
                        "key": self.time_millis(),
                        "value": {
                            "temperature": self.temp,
                            "status": self.status
                        }
                    }
                ]
            }
            ),
        )
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error(e)

        logger.debug(f"""sent weather data to kafka, temp: {self.temp},
        status: {self.status}""")
