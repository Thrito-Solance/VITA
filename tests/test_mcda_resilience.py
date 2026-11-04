"""Boundary, numerical stability, and determinism tests for MCDA-BB v3."""
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) 2026 thrito-solance
from __future__ import annotations

import math
from typing import Any

import pytest

from vita.mcda_core import (
    ALPHA,
    BETA,
    InvalidScoreBoundaryError,
    beta_cdf,
    beta_pdf,
    normalize_score,
)
from vita.redrive import compute_business_hash
from vita.schema import validate_and_derive


def valid_payload(intent_score: Any = 8.84) -> dict[str, Any]:
    return {
        "user_id": "MCDA_TEST",
        "current_state": "ACTIVE",
        "target_state": "DORMANT",
        "event_time": 1_000.0,
        "state_entry_time": 900.0,
        "average_order_value": 250.0,
        "intent_score": intent_score,
    }


@pytest.mark.parametrize(
    "score",
    [-0.0001, 10.0001, "8.84", None, True,
     float("nan"), float("inf"), float("-inf")],
)
def test_invalid_scores_raise_domain_exception_in_core_and_schema(score: Any) -> None:
    with pytest.raises(InvalidScoreBoundaryError):
        normalize_score(score)

    with pytest.raises(InvalidScoreBoundaryError):
        validate_and_derive(valid_payload(score))


@pytest.mark.parametrize(
    ("score", "expected_cdf"),
    [
        (8.84, 0.7681),
        (8.82, 0.7609),
        (8.80, 0.7536),
        (8.79, 0.7499),
        (8.61, 0.6827),
        (8.03, 0.4681),
        (7.92, 0.4306),
        (7.19, 0.2255),
        (7.12, 0.2102),
        (5.49, 0.0266),
    ],
)
def test_reference_percentile_benchmarks(score: float, expected_cdf: float) -> None:
    assert beta_cdf(score) == expected_cdf


@pytest.mark.parametrize(("score", "expected"), [(0, 0.0), (10, 1.0)])
def test_closed_domain_boundaries_are_finite(score: int, expected: float) -> None:
    assert normalize_score(score) == expected
    assert beta_cdf(score) == expected
    assert beta_pdf(score) == 0.0
    assert math.isfinite(beta_pdf(score))


def test_negative_zero_has_canonical_normalized_representation() -> None:
    normalized = normalize_score(-0.0)
    assert normalized == 0.0
    assert math.copysign(1.0, normalized) == 1.0


@pytest.mark.parametrize("score", [0.000001, 9.999999])
def test_pdf_and_cdf_are_stable_near_boundaries(score: float) -> None:
    density = beta_pdf(score)
    percentile = beta_cdf(score)
    assert math.isfinite(density)
    assert density >= 0.0
    assert 0.0 <= percentile <= 1.0


def test_schema_adds_four_decimal_model_metadata() -> None:
    result = validate_and_derive(valid_payload(8.84))
    assert result["intent_score_normalized"] == 0.884
    assert result["intent_score_density"] == beta_pdf(8.84)
    assert result["intent_score_percentile"] == 0.7681
    for key in (
        "intent_score_normalized",
        "intent_score_density",
        "intent_score_percentile",
    ):
        value = result[key]
        assert isinstance(value, float)
        assert value == round(value, 4)


def test_schema_preserves_payloads_without_intent_score() -> None:
    payload = valid_payload()
    del payload["intent_score"]
    result = validate_and_derive(payload)
    assert "intent_score_normalized" not in result
    assert "intent_score_density" not in result
    assert "intent_score_percentile" not in result


def test_repeated_model_outputs_and_business_hash_are_deterministic() -> None:
    first = validate_and_derive(valid_payload(8.84))
    second = validate_and_derive(valid_payload(8.84))

    assert first["intent_score_normalized"] == second["intent_score_normalized"]
    assert first["intent_score_density"] == second["intent_score_density"]
    assert first["intent_score_percentile"] == second["intent_score_percentile"]
    assert compute_business_hash(first) == compute_business_hash(second)


def test_reference_shape_constants_are_pinned() -> None:
    assert ALPHA == 10.0361
    assert BETA == 2.5705
