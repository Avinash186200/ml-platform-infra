"""The serving model format: plain JSON, no pickle.

A standardised logistic regression is a handful of numbers, so it is stored as
JSON and scored with numpy. Loading a model therefore never executes code from
the artifact store, and the evaluation gate scores the exact object that the
model API will load.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass

import numpy as np

FORMAT = "standardised-logistic/v1"


@dataclass(frozen=True)
class LinearModel:
    features: list[str]
    mean: list[float]
    scale: list[float]
    coef: list[float]
    intercept: float
    trained_on: dict

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        X = np.asarray(X, dtype=float)
        z = (X - np.asarray(self.mean)) / np.asarray(self.scale)
        logit = z @ np.asarray(self.coef) + self.intercept
        return 1.0 / (1.0 + np.exp(-logit))

    def predict_one(self, values: dict[str, float]) -> float:
        x = np.array([[values[f] for f in self.features]], dtype=float)
        return float(self.predict_proba(x)[0])

    def to_json(self) -> str:
        return json.dumps({"format": FORMAT, **asdict(self)}, indent=2, sort_keys=True)

    @classmethod
    def from_json(cls, text: str | bytes) -> LinearModel:
        obj = json.loads(text)
        if obj.pop("format", None) != FORMAT:
            raise ValueError("unknown model format")
        model = cls(**obj)
        n = len(model.features)
        if not (len(model.mean) == len(model.scale) == len(model.coef) == n):
            raise ValueError("model arrays do not match the feature list")
        if any(s <= 0 for s in model.scale):
            raise ValueError("model scale must be positive")
        return model

    def model_id(self, snapshot_version: int) -> str:
        digest = hashlib.sha256(self.to_json().encode()).hexdigest()[:10]
        return f"m-v{snapshot_version:06d}-{digest}"
