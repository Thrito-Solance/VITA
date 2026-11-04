"""Pure bounded-Beta calculations for VITA intent scores."""
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) 2026 thrito-solance
from __future__ import annotations

import math

SCORE_MIN = 0.0
SCORE_MAX = 10.0
ALPHA = 10.0361
BETA = 2.5705

_BETA_LOG_NORMALIZER = math.lgamma(ALPHA) + math.lgamma(BETA) - math.lgamma(
    ALPHA + BETA
)
_CF_EPSILON = 3.0e-14
_CF_MAX_ITERATIONS = 256
_CF_MIN_DENOMINATOR = 1.0e-300


class InvalidScoreBoundaryError(ValueError):
    """An intent score is not a finite numeric value in the closed interval [0, 10]."""

    code = "PX_08"


def _validate_score(x: float) -> None:
    if not isinstance(x, (int, float)) or isinstance(x, bool):
        raise InvalidScoreBoundaryError(
            f"intent_score must be an int or float, got {type(x).__name__}"
        )
    try:
        finite = math.isfinite(x)
    except OverflowError:
        finite = False
    if not finite:
        raise InvalidScoreBoundaryError("intent_score must be finite")
    if not SCORE_MIN <= x <= SCORE_MAX:
        raise InvalidScoreBoundaryError(
            f"intent_score must be in [{SCORE_MIN}, {SCORE_MAX}], got {x!r}"
        )


def normalize_score(x: float) -> float:
    """Map a validated score in [0, 10] to [0, 1], rounded to four decimals."""
    _validate_score(x)
    assert isinstance(x, (int, float))
    assert 0.0 <= x <= 10.0
    normalized = round(x / SCORE_MAX, 4)
    return 0.0 if normalized == 0.0 else normalized


def beta_pdf(x: float) -> float:
    """Evaluate the scaled Beta(alpha, beta) density at x in [0, 10]."""
    _validate_score(x)
    assert isinstance(x, (int, float))
    assert 0.0 <= x <= 10.0

    y = x / SCORE_MAX
    if y == 0.0 or y == 1.0:
        return 0.0

    log_density = (
        (ALPHA - 1.0) * math.log(y)
        + (BETA - 1.0) * math.log1p(-y)
        - _BETA_LOG_NORMALIZER
        - math.log(SCORE_MAX)
    )
    return round(math.exp(log_density), 4)


def _beta_continued_fraction(a: float, b: float, x: float) -> float:
    """Evaluate the incomplete-beta continued fraction with Lentz's method."""
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < _CF_MIN_DENOMINATOR:
        d = _CF_MIN_DENOMINATOR
    d = 1.0 / d
    h = d

    for m in range(1, _CF_MAX_ITERATIONS + 1):
        m_float = float(m)
        m2 = 2.0 * m_float
        aa = m_float * (b - m_float) * x / (
            (qam + m2) * (a + m2)
        )
        d = 1.0 + aa * d
        if abs(d) < _CF_MIN_DENOMINATOR:
            d = _CF_MIN_DENOMINATOR
        c = 1.0 + aa / c
        if abs(c) < _CF_MIN_DENOMINATOR:
            c = _CF_MIN_DENOMINATOR
        d = 1.0 / d
        h *= d * c

        aa = -(a + m_float) * (qab + m_float) * x / (
            (a + m2) * (qap + m2)
        )
        d = 1.0 + aa * d
        if abs(d) < _CF_MIN_DENOMINATOR:
            d = _CF_MIN_DENOMINATOR
        c = 1.0 + aa / c
        if abs(c) < _CF_MIN_DENOMINATOR:
            c = _CF_MIN_DENOMINATOR
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) <= _CF_EPSILON:
            return h

    raise ArithmeticError("regularized incomplete Beta calculation did not converge")


def beta_cdf(x: float) -> float:
    """Evaluate the regularized Beta CDF at x in [0, 10], rounded to four decimals."""
    _validate_score(x)
    assert isinstance(x, (int, float))
    assert 0.0 <= x <= 10.0

    y = x / SCORE_MAX
    if y == 0.0:
        return 0.0
    if y == 1.0:
        return 1.0

    front = math.exp(
        -_BETA_LOG_NORMALIZER
        + ALPHA * math.log(y)
        + BETA * math.log1p(-y)
    )
    if y < (ALPHA + 1.0) / (ALPHA + BETA + 2.0):
        probability = (
            front * _beta_continued_fraction(ALPHA, BETA, y) / ALPHA
        )
    else:
        probability = 1.0 - (
            front * _beta_continued_fraction(BETA, ALPHA, 1.0 - y) / BETA
        )

    if not math.isfinite(probability):
        raise ArithmeticError("regularized incomplete Beta result is non-finite")
    return round(min(1.0, max(0.0, probability)), 4)
