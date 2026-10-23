"""Alarm rules, with hysteresis so sensor noise near a threshold does not make
an alarm flicker on and off (which would also inflate the alarm count).

Rule alarm (baseline, current level):  HIGH_LEVEL on >= 95 %, clears < 93 %
                                       LOW_LEVEL  on <= 10 %, clears > 12 %
AI alarm (predicted level in 30 s):    OVERFLOW_RISK  on >= 90 %, clears < 88 %
                                       LOW_LEVEL_RISK on <= 15 %, clears > 17 %
"""

from __future__ import annotations

from dataclasses import dataclass

NONE = "NONE"


@dataclass(frozen=True)
class Band:
    high_name: str
    high_on: float
    high_off: float
    low_name: str
    low_on: float
    low_off: float

    def classify(self, value: float | None, previous: str) -> str:
        if value is None:
            return NONE
        if previous == self.high_name and value >= self.high_off:
            return self.high_name
        if previous == self.low_name and value <= self.low_off:
            return self.low_name
        if value >= self.high_on:
            return self.high_name
        if value <= self.low_on:
            return self.low_name
        return NONE


RULE = Band("HIGH_LEVEL", 95.0, 93.0, "LOW_LEVEL", 10.0, 12.0)
AI = Band("OVERFLOW_RISK", 90.0, 88.0, "LOW_LEVEL_RISK", 15.0, 17.0)
