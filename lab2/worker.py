import logging
from typing import Any, Optional

from base import Worker
from mrds import MyRedis
from constants import IN

class WcWorker(Worker):
  def run(self, **kwargs: Any) -> None:
    rds: MyRedis = kwargs['rds']
    filename : Optional[str] = rds.get_file(self.name)
    while filename is not None:
      filename = rds.get_file(self.name)
    logging.info("Exiting")