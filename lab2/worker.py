import logging
from typing import Any, Optional, List, Dict

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
    filename : Optional[str] = rds.get_file(self.name)
    while filename is not None:
      counts = wc(filename)
      rds.add_words(counts)
      print(f"[Worker {self.name}]: {filename}")
      filename = rds.get_file(self.name)
    logging.info("Exiting")