import logging
from typing import Any

from base import Worker
from mrds import MyRedis


class WcWorker(Worker):
  def run(self, **kwargs: Any) -> None:
    rds: MyRedis = kwargs['rds']
    logging.info("Exiting")