import logging
from typing import Any, Optional, List, Dict, Tuple, Union

from base import Worker
from mrds import MyRedis
from constants import IN

import re

"""
Clean a text file and return the list of words.
"""
def clean(inp: str) -> List[str]:
    inp = inp.lower()
    matches = re.findall(r'(\b[^\s]+\b)', inp)
    return matches

"""
Return the word count dictionary for a single file.
"""
def wc(filename: str) -> Dict[str,int]:
    counts = dict()
    text = open(filename, 'r').read()
    text = clean(text)
    for word in text:
        if word in counts:
            counts[word] = counts[word]+1
        else:
            counts[word] = 1
    return counts

class WcWorker(Worker):
  def run(self, **kwargs: Any) -> None:
    rds: MyRedis = kwargs['rds']
    record : Tuple[Union[str,None],Union[str,None]] = rds.get_file(self.name)
    msg_id : Union[str,None] = record[0]
    filename : Union[str,None] = record[1]
    while filename is not None:
      counts = wc(filename)
      rds.add_words(counts)
      rds.ack_msg(msg_id)
      logging.debug(f"[Worker {self.name}]: {filename} acked")
      record = rds.get_file(self.name)
      msg_id = record[0]
      filename = record[1]
    logging.info("Exiting")