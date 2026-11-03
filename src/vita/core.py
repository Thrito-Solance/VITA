"""VITA core: tensor core, intent payload, temporal kernel. No I/O."""
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) 2026 thrito-solance
from __future__ import annotations

import math
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Callable, Dict, Tuple


def _is_finite_number(value: Any) -> bool:
    try:
        return math.isfinite(value)
    except (OverflowError, TypeError):
        return False


class ContractViolation(Exception):
    """Typed contract violation. `code` is a PX_* flag."""
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class IntentPayload:
    action_key: str
    why_rationale: str
    rule_version: str = "v1"

    def __post_init__(self) -> None:
        if not self.action_key or not self.action_key.strip():
            raise ContractViolation("PX_07", "action_key must be non-empty")
        if not self.why_rationale or not self.why_rationale.strip():
            raise ContractViolation("PX_07", "why_rationale must be non-empty")
        if not self.rule_version or not self.rule_version.strip():
            raise ContractViolation("PX_07", "rule_version must be non-empty")


class TemporalKernel:
    @staticmethod
    def exponential_decay(t: float, p_max: float, lambda_rate: float) -> float:
        values = (("t", t), ("p_max", p_max), ("lambda_rate", lambda_rate))
        for name, value in values:
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise ContractViolation(
                    "PX_04", f"{name} must be numeric, got {type(value).__name__}"
                )
            if not _is_finite_number(value):
                raise ContractViolation("PX_06", f"{name} is non-finite: {value!r}")
        if t < 0:
            raise ContractViolation("PX_03", f"t is negative: {t}")
        if not 0.0 <= p_max <= 1.0 or lambda_rate < 0.0:
            raise ContractViolation("PX_04", "p_max must be in [0, 1] and lambda_rate non-negative")
        p = p_max * (1.0 - math.exp(-lambda_rate * t))
        if not math.isfinite(p):
            raise ContractViolation("PX_06", f"computed probability is non-finite: {p!r}")
        return p


class NeuroSymbolicTensorCore:
    def __init__(self) -> None:
        self._matrix: Dict[Tuple[int, int], Callable[[float], Tuple[float, IntentPayload]]] = {}
        state_registry = {
            "ACTIVE": 0, "AT_RISK": 1, "EMERGING_VIP": 2, "DORMANT": 3
        }
        self.state_registry = MappingProxyType(state_registry)
        self.reverse_registry = MappingProxyType(
            {value: state for state, value in state_registry.items()}
        )

    def register_transition_logic(
        self, from_state: str, to_state: str,
        rule: Callable[[float], Tuple[float, IntentPayload]],
    ) -> None:
        if not isinstance(from_state, str) or from_state not in self.state_registry:
            raise ContractViolation("PX_05", f"unknown from_state {from_state!r}")
        if not isinstance(to_state, str) or to_state not in self.state_registry:
            raise ContractViolation("PX_05", f"unknown to_state {to_state!r}")
        if not callable(rule):
            raise ContractViolation("PX_07", "transition rule must be callable")
        key = (self.state_registry[from_state], self.state_registry[to_state])
        if key in self._matrix:
            raise ContractViolation(
                "PX_05", f"transition {from_state} -> {to_state} is already registered"
            )
        self._matrix[key] = rule

    def evaluate(self, record: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(record, dict):
            return self._quarantine({}, "PX_04", "record must be an object")

        current = record.get("current_state")
        target = record.get("target_state")
        t = record.get("time_in_state")
        aov = record.get("average_order_value")

        if not isinstance(current, str) or current not in self.state_registry:
            return self._quarantine(record, "PX_05", f"Unknown current_state {current!r}")
        if not isinstance(target, str) or target not in self.state_registry:
            return self._quarantine(record, "PX_05", f"Unknown target_state {target!r}")

        u = self.state_registry[current]
        v = self.state_registry[target]

        if not isinstance(t, (int, float)) or isinstance(t, bool):
            return self._quarantine(record, "PX_04", "time_in_state must be numeric")
        if not _is_finite_number(t):
            return self._quarantine(record, "PX_06", f"time_in_state is non-finite: {t!r}")
        if t < 0:
            return self._quarantine(record, "PX_03", f"time_in_state is negative: {t}")

        if (u, v) not in self._matrix:
            return self._quarantine(record, "PX_02", f"No registered edge {current} -> {target}")

        if not isinstance(aov, (int, float)) or isinstance(aov, bool):
            return self._quarantine(record, "PX_04", "average_order_value must be numeric")
        if not _is_finite_number(aov):
            return self._quarantine(record, "PX_06", f"average_order_value is non-finite: {aov!r}")

        try:
            probability, intent = self._matrix[(u, v)](t)
        except ContractViolation as exc:
            return self._quarantine(record, exc.code, str(exc))

        if not isinstance(probability, (int, float)) or isinstance(probability, bool):
            return self._quarantine(record, "PX_04", "transition probability must be numeric")
        if not _is_finite_number(probability) or not 0.0 <= probability <= 1.0:
            return self._quarantine(record, "PX_06", f"invalid transition probability: {probability!r}")
        if not isinstance(intent, IntentPayload):
            return self._quarantine(record, "PX_07", "transition rule returned an invalid intent")

        if target == "EMERGING_VIP" and aov < 100.0:
            return self._quarantine(record, "PX_01", f"VIP escalation with AOV {aov} < 100.0")

        return {
            "status": "PROCESSED_SUCCESSFULLY",
            "intent": intent,
            "metadata": {
                "where_from": current,
                "where_to": target,
                "computed_probability": round(probability, 4),
                "why": intent.why_rationale,
                "what": intent.action_key,
                "rule_version": intent.rule_version,
            },
        }

    @staticmethod
    def _quarantine(record: Dict[str, Any], flag: str, rationale: str) -> Dict[str, Any]:
        return {
            "status": "PARADOX_QUARANTINE_HANDOFF",
            "telemetry": {"flag": flag, "system_rationale": rationale,
                          "offending_payload": record},
        }
