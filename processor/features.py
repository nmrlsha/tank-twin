"""Feature engineering shared by the live processor and the AI training script.

Keeping one copy of this code guarantees the model sees exactly the same
inputs in training and in operation.
"""

from __future__ import annotations

from collections import deque

# Order matters: the model is trained on columns in this order.
FEATURES = ("level_pct", "inflow_lpm", "outflow_lpm", "pump_on",
            "net_flow_lpm", "d_level_5s")

HORIZON_S = 30        # prediction target: level 30 s ahead
D_LEVEL_WINDOW_S = 5  # d_level_5s looks back this far
MAX_GAP_S = 3.0       # a longer gap between samples counts as an interruption


def make_features(level_pct: float, inflow_lpm: float, outflow_lpm: float,
                  pump_on: bool, d_level_5s: float) -> list[float]:
    """One feature row, in FEATURES order."""
    return [level_pct, inflow_lpm, outflow_lpm, 1.0 if pump_on else 0.0,
            inflow_lpm - outflow_lpm, d_level_5s]


class LevelHistory:
    """Recent (time, level) samples, used to compute d_level_5s.

    Returns None from d_level() until 5 s of continuous history exist.
    After a gap longer than MAX_GAP_S (stream interruption) the history is
    cleared, because a level change across the gap would be meaningless.
    """

    def __init__(self, window_s: float = D_LEVEL_WINDOW_S, max_gap_s: float = MAX_GAP_S):
        self.window_s = window_s
        self.max_gap_s = max_gap_s
        self._samples: deque[tuple[float, float]] = deque()

    def add(self, t: float, level: float) -> None:
        if self._samples:
            last_t = self._samples[-1][0]
            if t <= last_t:          # duplicate or out-of-order: ignore
                return
            if t - last_t > self.max_gap_s:
                self._samples.clear()
        self._samples.append((t, level))
        # Keep a little more than the window.
        while self._samples and self._samples[0][0] < t - self.window_s - 3:
            self._samples.popleft()

    def d_level(self) -> float | None:
        if not self._samples:
            return None
        t_now, level_now = self._samples[-1]
        target = t_now - self.window_s
        # Oldest sample that is at least ~window_s old.
        past = None
        for t, lv in self._samples:
            if t <= target + 0.5:
                past = (t, lv)
            else:
                break
        if past is None:
            return None
        return level_now - past[1]

    def __len__(self) -> int:
        return len(self._samples)
