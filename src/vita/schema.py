"""Schema gate. Derives `time_in_state` from event time server-side."""
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) 2026 thrito-solance
from __future__ import annotations

import math
from typing import Any, Dict

from vita.core import ContractViolation
from vita.mcda_core import (
    InvalidScoreBoundaryError,
    beta_cdf,
    beta_pdf,
    normalize_score,
)

REQUIRED = (
    "user_id", "current_state", "target_state",
    "event_time", "state_entry_time", "average_order_value",
)


def _is_finite_number(value: int | float) -> bool:
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def validate_and_derive(raw: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise ContractViolation("PX_04", "payload must be an object")

    missing = [f for f in REQUIRED if f not in raw or raw[f] is None]
    if missing:
        raise ContractViolation("PX_04", f"missing/null fields: {missing}")

    for f in ("user_id", "current_state", "target_state"):
        if not isinstance(raw[f], str) or not raw[f].strip():
            raise ContractViolation("PX_04", f"{f} must be a non-empty string")

    for f in ("event_time", "state_entry_time", "average_order_value"):
        v = raw[f]
        if not isinstance(v, (int, float)) or isinstance(v, bool):
            raise ContractViolation("PX_04", f"{f} must be numeric, got {type(v).__name__}")
        if not _is_finite_number(v):
            raise ContractViolation("PX_06", f"{f} is non-finite: {v!r}")

    if raw["event_time"] < raw["state_entry_time"]:
        raise ContractViolation("PX_03", "event_time precedes state_entry_time")

    derived = dict(raw)
    derived["time_in_state"] = float(raw["event_time"]) - float(raw["state_entry_time"])
    if "intent_score" in raw:
        normalized = normalize_score(raw["intent_score"])
        density = beta_pdf(raw["intent_score"])
        percentile = beta_cdf(raw["intent_score"])
        derived.update({
            "intent_score_normalized": normalized,
            "intent_score_density": density,
            "intent_score_percentile": percentile,
        })
    return derived
