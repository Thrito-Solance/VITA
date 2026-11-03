"""Adversarial resilience suite. All tests run on memory + redis (via `backend`)."""
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) 2026 thrito-solance
from __future__ import annotations

import concurrent.futures
import threading
import time
from dataclasses import FrozenInstanceError
from typing import Any, Dict, List, Optional

import pytest

from vita.core import ContractViolation, IntentPayload, NeuroSymbolicTensorCore, TemporalKernel
from vita.persistence import (
    IdempotencyStore, InMemoryBackend, LedgerEntry, LedgerReceipt,
    LedgerWriteError,
    TransitionLedger, compute_payload_hash,
)
from vita.redrive import (
    Keys, QuarantineError, QuarantineRecord, QuarantineStore,
    ReDriveAlreadyDone, ReDriveBusy, ReDrivePipeline, ReDriveReceipt,
    ReDriveRejected, compute_business_hash, idempotency_key_for,
)
from vita.schema import validate_and_derive


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_engine(with_edge: bool = True) -> NeuroSymbolicTensorCore:
    engine = NeuroSymbolicTensorCore()

    def active_to_dormant(t):
        p = TemporalKernel.exponential_decay(t, p_max=0.45, lambda_rate=0.04)
        return p, IntentPayload(
            action_key="TRIGGER_WINBACK_SEQUENCE",
            why_rationale=f"Active user broke habit at t={t:.2f}.",
            rule_version="v1",
        )

    def dormant_to_vip(t):
        return 1.0, IntentPayload(
            action_key="AUTHORIZE_VIP_UPGRADE",
            why_rationale="Dormant account premium scale vector.",
            rule_version="v1",
        )

    engine.register_transition_logic("ACTIVE", "DORMANT", active_to_dormant)
    if with_edge:
        engine.register_transition_logic("DORMANT", "EMERGING_VIP", dormant_to_vip)
    return engine


def valid_raw(user_id="U1", event_time=1_000.0, state_entry_time=900.0,
              aov=250.0, current="ACTIVE", target="DORMANT") -> Dict[str, Any]:
    return {
        "user_id": user_id,
        "event_time": event_time,
        "state_entry_time": state_entry_time,
        "average_order_value": aov,
        "current_state": current,
        "target_state": target,
    }


class ListInjector:
    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

    def __call__(self, topic, key, payload, headers):
        self.calls.append({"topic": topic, "key": key,
                           "payload": payload, "headers": dict(headers)})


def process_via_consumer_path(raw, engine, quarantine):
    try:
        validated = validate_and_derive(raw)
    except ContractViolation as exc:
        rec = quarantine.quarantine(raw, exc.code, str(exc))
        return {"routed": "quarantine", "record": rec}
    result = engine.evaluate(validated)
    if result["status"] == "PROCESSED_SUCCESSFULLY":
        return {"routed": "processed", "result": result}
    t = result["telemetry"]
    rec = quarantine.quarantine(raw, t["flag"], t["system_rationale"])
    return {"routed": "quarantine", "record": rec}


# =========================================================================
# §1  NaN INVARIANT
# =========================================================================

@pytest.mark.parametrize("field,value,expected_flag", [
    ("average_order_value", float("nan"),  "PX_06"),
    ("average_order_value", float("inf"),  "PX_06"),
    ("average_order_value", float("-inf"), "PX_06"),
    ("event_time",          float("nan"),  "PX_06"),
    ("event_time",          float("inf"),  "PX_06"),
    ("state_entry_time",    float("nan"),  "PX_06"),
    ("state_entry_time",    float("-inf"), "PX_06"),
    ("average_order_value", "250.0",       "PX_04"),
    ("average_order_value", None,          "PX_04"),
    ("event_time",          True,          "PX_04"),
    ("event_time",          "SHIFT",       "PX_03"),
])
def test_nan_and_inf_are_caught_by_the_gate(field, value, expected_flag):
    if value == "SHIFT":
        raw = valid_raw(event_time=100.0, state_entry_time=200.0)
    else:
        raw = valid_raw()
        raw[field] = value
    with pytest.raises(ContractViolation) as excinfo:
        validate_and_derive(raw)
    assert excinfo.value.code == expected_flag, (
        f"expected {expected_flag}, got {excinfo.value.code}: {excinfo.value}"
    )


def test_nan_would_bypass_vip_check_if_gate_were_removed():
    nan = float("nan")
    assert (nan < 100.0) is False
    assert (nan >= 100.0) is False
    raw = valid_raw(target="EMERGING_VIP", current="DORMANT", aov=nan)
    with pytest.raises(ContractViolation) as excinfo:
        validate_and_derive(raw)
    assert excinfo.value.code == "PX_06"


def test_nan_timestamp_never_reaches_temporal_kernel():
    raw = valid_raw(event_time=float("nan"))
    with pytest.raises(ContractViolation) as excinfo:
        validate_and_derive(raw)
    assert excinfo.value.code == "PX_06"

    with pytest.raises(ContractViolation) as excinfo:
        TemporalKernel.exponential_decay(float("nan"), p_max=0.45, lambda_rate=0.04)
    assert excinfo.value.code == "PX_06"

    with pytest.raises(ContractViolation) as excinfo:
        TemporalKernel.exponential_decay(float("inf"), p_max=0.45, lambda_rate=0.04)
    assert excinfo.value.code == "PX_06"


def test_direct_engine_calls_quarantine_malformed_values():
    engine = build_engine()
    malformed = {
        "current_state": [],
        "target_state": "DORMANT",
        "time_in_state": float("nan"),
        "average_order_value": 1.0,
    }
    outcome = engine.evaluate(malformed)
    assert outcome["status"] == "PARADOX_QUARANTINE_HANDOFF"
    assert outcome["telemetry"]["flag"] == "PX_05"

    malformed["current_state"] = "ACTIVE"
    outcome = engine.evaluate(malformed)
    assert outcome["telemetry"]["flag"] == "PX_06"

    vip_engine = build_engine()
    vip_record = {
        "current_state": "DORMANT",
        "target_state": "EMERGING_VIP",
        "time_in_state": 1.0,
        "average_order_value": float("nan"),
    }
    vip_outcome = vip_engine.evaluate(vip_record)
    assert vip_outcome["status"] == "PARADOX_QUARANTINE_HANDOFF"
    assert vip_outcome["telemetry"]["flag"] == "PX_06"


def test_temporal_kernel_rejects_invalid_probability_parameters():
    with pytest.raises(ContractViolation) as excinfo:
        TemporalKernel.exponential_decay(1.0, p_max=1.1, lambda_rate=0.1)
    assert excinfo.value.code == "PX_04"

    with pytest.raises(ContractViolation) as excinfo:
        TemporalKernel.exponential_decay(1.0, p_max=0.5, lambda_rate=-0.1)
    assert excinfo.value.code == "PX_04"


def test_transition_registry_rejects_mutation_and_duplicate_edges():
    engine = build_engine()
    with pytest.raises(TypeError):
        engine.state_registry["NEW"] = 4

    with pytest.raises(ContractViolation) as excinfo:
        engine.register_transition_logic(
            "ACTIVE", "DORMANT",
            lambda t: (0.5, IntentPayload("TEST", "duplicate")),
        )
    assert excinfo.value.code == "PX_05"


def test_nan_routed_to_quarantine_with_correct_flag(backend):
    engine = build_engine()
    qs = QuarantineStore(backend)
    outcome = process_via_consumer_path(valid_raw(aov=float("nan")), engine, qs)
    assert outcome["routed"] == "quarantine"
    rec = outcome["record"]
    assert rec.flag == "PX_06"

    listed = qs.inspect(flag="PX_06")
    assert len(listed) == 1
    assert listed[0].quarantine_id == rec.quarantine_id
    assert listed[0].truncated_payload_hash == rec.truncated_payload_hash
    assert len(listed[0].truncated_payload_hash) == 16


# =========================================================================
# §2  IDEMPOTENCY DEDUP — concurrency
# =========================================================================

def test_concurrent_duplicates_yield_exactly_one_ledger_entry(backend):
    store = IdempotencyStore(backend)
    ledger = TransitionLedger(backend)
    engine = build_engine()

    raw = valid_raw(user_id="RACE", event_time=500.0, state_entry_time=400.0)
    validated = validate_and_derive(raw)
    key = idempotency_key_for(validated)
    barrier = threading.Barrier(2)
    outcomes: List[str] = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        if not store.try_reserve(key):
            with lock:
                outcomes.append("short-circuited")
            return
        result = engine.evaluate(validated)
        assert result["status"] == "PROCESSED_SUCCESSFULLY"
        intent = result["intent"]
        ledger.append(LedgerEntry(
            user_id=validated["user_id"],
            event_time=validated["event_time"],
            from_state=validated["current_state"],
            to_state=validated["target_state"],
            rule_version=intent.rule_version,
            computed_probability=result["metadata"]["computed_probability"],
            why_rationale=intent.why_rationale,
            action_key=intent.action_key,
            payload_hash=compute_business_hash(validated),
        ))
        with lock:
            outcomes.append("processed")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        for f in [ex.submit(worker) for _ in range(2)]:
            f.result(timeout=10)

    assert sorted(outcomes) == ["processed", "short-circuited"]
    assert len(ledger.read_all()) == 1
    assert store.is_reserved(key) is True


def test_idempotency_ttl_does_not_leak_across_event_times(backend):
    store = IdempotencyStore(backend)
    p1 = validate_and_derive(valid_raw(user_id="U", event_time=100.0, state_entry_time=50.0))
    p2 = validate_and_derive(valid_raw(user_id="U", event_time=200.0, state_entry_time=150.0))
    k1, k2 = idempotency_key_for(p1), idempotency_key_for(p2)
    assert k1 != k2
    assert store.try_reserve(k1) is True
    assert store.try_reserve(k2) is True
    assert store.try_reserve(k1) is False
    assert store.try_reserve(k2) is False


# =========================================================================
# §3  RE-DRIVE RECOVERY
# =========================================================================

def test_px02_redrive_with_edge_registered_emits_downstream(backend):
    engine = build_engine(with_edge=False)
    qs = QuarantineStore(backend)
    original = valid_raw(user_id="REDRIVE_1", current="DORMANT",
                         target="EMERGING_VIP", event_time=1_000.0,
                         state_entry_time=900.0, aov=500.0)
    original_validated = validate_and_derive(original)
    original_key = idempotency_key_for(original_validated)
    idem = IdempotencyStore(backend)

    assert idem.try_reserve(original_key) is True
    result = engine.evaluate(original_validated)
    assert result["status"] == "PARADOX_QUARANTINE_HANDOFF"
    assert result["telemetry"]["flag"] == "PX_02"
    rec = qs.quarantine(original, result["telemetry"]["flag"],
                        result["telemetry"]["system_rationale"])

    engine.register_transition_logic(
        "DORMANT", "EMERGING_VIP",
        lambda t: (1.0, IntentPayload(
            action_key="AUTHORIZE_VIP_UPGRADE",
            why_rationale="DORMANT→EMERGING_VIP edge registered by ops.",
            rule_version="v1-hotfix",
        )),
    )

    injector = ListInjector()
    pipeline = ReDrivePipeline(
        quarantine=qs, engine=engine, backend=backend,
        injector=injector, input_topic="vita.raw.events",
    )
    receipt = pipeline.redrive(
        quarantine_id=rec.quarantine_id,
        corrected_payload=original,
        operator="ops-alice",
        reason="registered missing DORMANT→EMERGING_VIP edge",
    )
    assert isinstance(receipt, ReDriveReceipt)
    assert receipt.quarantine_id == rec.quarantine_id
    assert len(receipt.business_hash) == 64
    cached = pipeline.redrive(
        quarantine_id=rec.quarantine_id,
        corrected_payload=original,
        operator="ops-alice",
        reason="same request retry",
        redrive_id=receipt.redrive_id,
    )
    assert cached == receipt
    with pytest.raises(ReDriveRejected):
        pipeline.redrive(
            quarantine_id=rec.quarantine_id,
            corrected_payload=dict(original, average_order_value=501.0),
            operator="ops-alice",
            reason="reused request id",
            redrive_id=receipt.redrive_id,
        )

    assert len(injector.calls) == 1
    call = injector.calls[0]
    assert call["topic"] == "vita.raw.events"
    assert call["key"] == "REDRIVE_1"
    assert call["headers"]["re-driven"] == "true"
    assert call["headers"]["redrive-id"] == receipt.redrive_id
    assert call["headers"]["quarantine-id"] == rec.quarantine_id
    assert call["headers"]["business-hash"] == receipt.business_hash
    env = call["payload"]["_redrive"]
    assert env["redrive_id"] == receipt.redrive_id
    assert env["quarantine_id"] == rec.quarantine_id
    assert env["operator"] == "ops-alice"
    assert env["original_flag"] == "PX_02"

    redrive_key = idempotency_key_for(call["payload"])
    assert redrive_key.startswith(f"{Keys.REDRIVE_IDEM}:")
    assert redrive_key != original_key
    assert idem.is_reserved(original_key) is True    # untouched
    assert idem.is_reserved(redrive_key) is True     # claimed by pipeline

    with pytest.raises(ReDriveAlreadyDone):
        pipeline.redrive(
            quarantine_id=rec.quarantine_id,
            corrected_payload=original,
            operator="ops-bob", reason="retry",
        )
    assert len(injector.calls) == 1

    audit = backend.xrange(Keys.REDRIVE_AUDIT_STREAM)
    assert len(audit) == 1
    assert audit[0][1]["redrive_id"] == receipt.redrive_id
    assert audit[0][1]["operator"] == "ops-alice"


def test_redrive_rejects_corrected_payload_that_still_fails_gates(backend):
    engine = build_engine(with_edge=False)
    qs = QuarantineStore(backend)
    rec = qs.quarantine(valid_raw(current="DORMANT", target="EMERGING_VIP"),
                        "PX_02", "edge missing")
    injector = ListInjector()
    pipeline = ReDrivePipeline(
        quarantine=qs, engine=engine, backend=backend,
        injector=injector, input_topic="vita.raw.events",
    )

    with pytest.raises(ReDriveRejected):
        pipeline.redrive(rec.quarantine_id,
                         valid_raw(current="DORMANT", target="EMERGING_VIP",
                                   aov=float("nan")),
                         operator="ops", reason="bad fix")
    assert len(injector.calls) == 0

    with pytest.raises(ReDriveRejected):
        pipeline.redrive(rec.quarantine_id,
                         valid_raw(current="DORMANT", target="EMERGING_VIP",
                                   event_time=100.0, state_entry_time=200.0),
                         operator="ops", reason="bad fix")
    assert len(injector.calls) == 0

    with pytest.raises(ReDriveRejected):
        pipeline.redrive(rec.quarantine_id,
                         valid_raw(current="DORMANT", target="EMERGING_VIP", aov=500.0),
                         operator="ops", reason="edge never registered")
    assert len(injector.calls) == 0

    assert backend.exists(Keys.lock(rec.quarantine_id)) == 0


# =========================================================================
# §4  LEDGER AUDIT
# =========================================================================

def test_redrive_surfaces_audit_failure_after_injection():
    class AuditFailureBackend(InMemoryBackend):
        def xadd(self, name, fields, *, maxlen=None):
            if name == Keys.REDRIVE_AUDIT_STREAM:
                raise OSError("audit unavailable")
            return super().xadd(name, fields, maxlen=maxlen)

    backend = AuditFailureBackend()
    quarantine = QuarantineStore(backend)
    original = valid_raw()
    record = quarantine.quarantine(original, "PX_02", "re-drive test")
    injector = ListInjector()
    pipeline = ReDrivePipeline(
        quarantine=quarantine,
        engine=build_engine(),
        backend=backend,
        injector=injector,
        input_topic="vita.raw.events",
    )

    with pytest.raises(QuarantineError, match="delivery outcome is uncertain"):
        pipeline.redrive(
            record.quarantine_id,
            original,
            operator="ops",
            reason="verify audit failure",
            redrive_id="audit-failure-test",
        )

    assert len(injector.calls) == 1
    assert backend.exists(Keys.idem("audit-failure-test")) == 0


def test_ledger_entry_carries_full_audit_trail(backend):
    engine = build_engine()
    ledger = TransitionLedger(backend)
    validated = validate_and_derive(valid_raw(
        user_id="AUDIT_1", event_time=1_000.0, state_entry_time=900.0,
    ))
    result = engine.evaluate(validated)
    assert result["status"] == "PROCESSED_SUCCESSFULLY"
    meta = result["metadata"]
    intent = result["intent"]

    entry = LedgerEntry(
        user_id=validated["user_id"],
        event_time=validated["event_time"],
        from_state=validated["current_state"],
        to_state=validated["target_state"],
        rule_version=intent.rule_version,
        computed_probability=meta["computed_probability"],
        why_rationale=intent.why_rationale,
        action_key=intent.action_key,
        payload_hash=compute_business_hash(validated),
    )
    receipt = ledger.append(entry)
    assert isinstance(receipt, LedgerReceipt)
    assert receipt.duplicate is False
    assert receipt.payload_hash == entry.payload_hash

    entries = ledger.read_all()
    assert len(entries) == 1
    e = entries[0]
    assert e.why_rationale == intent.why_rationale
    assert e.rule_version == intent.rule_version
    assert e.computed_probability == meta["computed_probability"]
    assert e.payload_hash == compute_business_hash(validated)
    assert len(e.payload_hash) == 64


def test_ledger_rejects_mutation_attempts(backend):
    entry = LedgerEntry(
        user_id="X", event_time=1.0, from_state="ACTIVE", to_state="DORMANT",
        rule_version="v1", computed_probability=0.5,
        why_rationale="test", action_key="TEST", payload_hash="a" * 64,
    )
    with pytest.raises(FrozenInstanceError):
        entry.why_rationale = "tampered"   # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        entry.payload_hash = "b" * 64      # type: ignore[misc]


def test_ledger_is_idempotent_by_business_hash(backend):
    ledger = TransitionLedger(backend)
    p = {"user_id": "U", "event_time": 1.0, "average_order_value": 100.0}
    p_reordered = {"average_order_value": 100.0, "event_time": 1.0, "user_id": "U"}
    assert compute_payload_hash(p) == compute_payload_hash(p_reordered)

    entry = LedgerEntry(
        user_id="U", event_time=1.0, from_state="ACTIVE", to_state="DORMANT",
        rule_version="v1", computed_probability=0.25,
        why_rationale="test", action_key="TEST",
        payload_hash=compute_payload_hash(p),
    )
    r1 = ledger.append(entry)
    r2 = ledger.append(entry)
    assert r1.duplicate is False
    assert r2.duplicate is True
    assert r2.entry_id == r1.entry_id
    assert len(ledger.read_all()) == 1

    p2 = dict(p, average_order_value=101.0)
    e2 = LedgerEntry(
        user_id="U", event_time=1.0, from_state="ACTIVE", to_state="DORMANT",
        rule_version="v1", computed_probability=0.25,
        why_rationale="test", action_key="TEST",
        payload_hash=compute_payload_hash(p2),
    )
    r3 = ledger.append(e2)
    assert r3.duplicate is False
    assert r3.entry_id != r1.entry_id
    assert len(ledger.read_all()) == 2


def test_ledger_hash_is_cryptographic_and_stable(backend):
    base = {"user_id": "U", "event_time": 1.0, "aov": 100.0}
    h1 = compute_payload_hash(base)
    h2 = compute_payload_hash(base)
    assert h1 == h2
    assert len(h1) == 64
    int(h1, 16)

    for mutated in (
        dict(base, user_id="V"),
        dict(base, event_time=1.001),
        dict(base, aov=100.0001),
        dict(base, extra="x"),
    ):
        assert compute_payload_hash(mutated) != h1


def test_atomic_ledger_append_deduplicates_concurrent_retries():
    backend = InMemoryBackend()
    ledger = TransitionLedger(backend)
    entry = LedgerEntry(
        user_id="RACE", event_time=500.0, from_state="ACTIVE",
        to_state="DORMANT", rule_version="v1",
        computed_probability=0.25, why_rationale="retry test",
        action_key="TEST", payload_hash="f" * 64,
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        receipts = list(executor.map(lambda _: ledger.append(entry), range(32)))

    assert sum(not receipt.duplicate for receipt in receipts) == 1
    assert len({receipt.entry_id for receipt in receipts}) == 1
    assert len(ledger.read_all()) == 1


def test_ledger_audit_fails_on_malformed_entry_instead_of_skipping_it():
    backend = InMemoryBackend()
    backend.xadd(TransitionLedger.STREAM_NAME, {"entry_id": "incomplete"})
    with pytest.raises(LedgerWriteError, match="malformed ledger entry"):
        TransitionLedger(backend).read_all()


def test_quarantine_hash_supports_non_finite_rejected_input():
    payload = valid_raw(aov=float("nan"))
    store = QuarantineStore(InMemoryBackend())
    quarantined = store.quarantine(payload, "PX_06", "non-finite input")
    restored = store.load_payload(quarantined.quarantine_id)
    assert quarantined.truncated_payload_hash == compute_payload_hash(restored)[:16]
