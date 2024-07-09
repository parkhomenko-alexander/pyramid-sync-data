from dataclasses import dataclass
from typing import Literal


@dataclass
class TimeRangeForDataSync():
    start: str
    end: str

TimePartition = Literal["5m", "30m", "2h", "1day", "1month"]
