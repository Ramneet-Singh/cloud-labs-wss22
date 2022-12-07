from typing import Final

LOGFILE: Final[str] = "/tmp/wc.log"
N_WORKERS: Final[int] = 4
GLOB: Final[str] = "../lab1/data/*utf-8"
IN: Final[bytes] = b"files"
FNAME: Final[bytes] = b"fname"
COUNT: Final[bytes] = b"count"
GROUP: Final[str] = "worker"
