import logging
import time
from dataclasses import dataclass
from random import randint

import boto3 as boto3

from daggerml._core import get_client as get_client

logger = logging.getLogger(__name__)


def current_time_millis():
    return round(time.time() * 1000)


@dataclass
class BackoffWithJitter:
    min: int = 100
    max: int = 10000
    k: int = 2
    state: int = 0

    def __call__(self):
        if self.state == 0:
            self.state = randint(self.min, self.min * 2)
            return self.state
        self.state = min(self.max, randint(self.min, max(self.min, self.state) * self.k))
        return self.state
