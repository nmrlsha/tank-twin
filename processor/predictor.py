"""Loads the trained level-prediction model and runs it.

The model file is produced by the training script in ai/ (next steps) as a
joblib dict:  {"model": estimator, "features": [...], "horizon_s": 30,
"version": "..."}

If no model file exists yet, predictions are simply unavailable
(model_status = MODEL_MISSING) and everything else keeps working. The file is
re-checked periodically, so a newly trained model is picked up without
restarting the processor.
"""

from __future__ import annotations

import logging
import math
import os
import time

from features import FEATURES

log = logging.getLogger("predictor")

# model_status values published in each processed message
OK = "OK"
WARMING_UP = "WARMING_UP"          # less than 5 s of continuous history
MODEL_MISSING = "MODEL_MISSING"    # no trained model file yet
MODEL_ERROR = "MODEL_ERROR"        # model failed to load or predict
INPUT_INVALID = "INPUT_INVALID"    # a feature is missing or not finite


class Predictor:
    def __init__(self, path: str, check_every_s: float = 10.0):
        self.path = path
        self.check_every_s = check_every_s
        self.model = None
        self.version: str | None = None
        self._mtime: float | None = None
        self._last_check = 0.0
        self._load_error = False
        self.reload_if_changed(force=True)

    def reload_if_changed(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_check < self.check_every_s:
            return
        self._last_check = now
        try:
            mtime = os.path.getmtime(self.path)
        except OSError:
            if self.model is not None:
                log.warning("model file %s disappeared; keeping loaded model", self.path)
            return
        if mtime == self._mtime:
            return
        self._mtime = mtime
        try:
            import joblib
            bundle = joblib.load(self.path)
            if tuple(bundle["features"]) != FEATURES:
                raise ValueError(f"model features {bundle['features']} != {FEATURES}")
            self.model = bundle["model"]
            self.version = str(bundle.get("version", "unknown"))
            self._load_error = False
            log.info("loaded model %s (version %s)", self.path, self.version)
        except Exception as exc:  # noqa: BLE001 - any load failure is reported, not fatal
            self._load_error = True
            log.error("could not load model %s: %s", self.path, exc)

    def predict(self, features: list[float] | None) -> tuple[float | None, str]:
        """Returns (predicted level in %, model_status)."""
        self.reload_if_changed()
        if features is None:
            return None, WARMING_UP
        if self.model is None:
            return None, MODEL_ERROR if self._load_error else MODEL_MISSING
        if len(features) != len(FEATURES) or not all(
                isinstance(v, (int, float)) and math.isfinite(v) for v in features):
            return None, INPUT_INVALID
        try:
            import numpy as np
            y = float(self.model.predict(np.asarray([features], dtype=float))[0])
        except Exception as exc:  # noqa: BLE001
            log.error("prediction failed: %s", exc)
            return None, MODEL_ERROR
        if not math.isfinite(y):
            return None, MODEL_ERROR
        # A level outside the tank is physically impossible; clip.
        return round(min(100.0, max(0.0, y)), 2), OK
