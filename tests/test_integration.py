"""Cross-process invariants. Requires `make up` (kafka + redis healthy)."""
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) 2026 thrito-solance
from __future__ import annotations

import json
import os
import time
import uuid
from typing import Any, Dict, Optional

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def kafka_config() -> Dict[str, str]:
    return {
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
        "group.id": f"pytest-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    }


@pytest.fixture(scope="module")
def redis_url() -> str:
    url = os.environ.get("REDIS_URL")
    if not url:
        pytest.skip("REDIS_URL not set")
    return url


@pytest.fixture
def producer(kafka_config):
    from confluent_kafka import Producer
    p = Producer({
        "bootstrap.servers": kafka_config["bootstrap.servers"],
        "enable.idempotence": True,
    })
    yield p
    p.flush(timeout=10)


def _consume_until(consumer, predicate, timeout: float = 30.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        msg = consumer.poll(timeout=0.5)
        if msg is None or msg.error():
            continue
        hit = predicate(msg)
        if hit is not None:
            return hit
    pytest.fail(f"timeout after {timeout}s")


def test_nan_aov_lands_in_dlq_with_px06(producer, kafka_config):
    from confluent_kafka import Consumer

    # Inline pre-flight: prove the gate itself rejects NaN with PX_06.
    from vita.schema import validate_and_derive
    from vita.core import ContractViolation
    with pytest.raises(ContractViolation) as excinfo:
        validate_and_derive({
            "user_id": "preflight",
            "event_time": 1_700_000_000.0,
            "state_entry_time": 1_700_000_000.0 - 60.0,
            "average_order_value": float("nan"),
            "current_state": "ACTIVE", "target_state": "DORMANT",
        })
    assert excinfo.value.code == "PX_06"

    raw_topic = os.environ.get("VITA_INPUT_TOPIC", "vita.raw.events")
    dlq_topic = os.environ.get("VITA_DLQ_TOPIC", "vita.paradox.quarantine")

    consumer = Consumer({
        **kafka_config,
        "group.id": f"pytest-dlq-{uuid.uuid4().hex[:8]}",
    })
    consumer.subscribe([dlq_topic])
    consumer.poll(timeout=1.0)  # prime assignment

    user_id = f"NaN_{uuid.uuid4().hex[:8]}"
    now = time.time()
    poison = {
        "user_id": user_id,
        "current_state": "ACTIVE",
        "target_state": "DORMANT",
        "event_time": now,
        "state_entry_time": now - 60.0,
        "average_order_value": float("nan"),
    }
    producer.produce(raw_topic, key=user_id,
                     value=json.dumps(poison, allow_nan=True))
    producer.flush(timeout=5)

    def match(msg) -> Optional[Dict[str, Any]]:
        try:
            body = json.loads(msg.value())
        except Exception:
            return None
        if body.get("telemetry", {}).get("offending_payload", {}).get("user_id") == user_id:
            return body
        return None

    hit = _consume_until(consumer, match)
    consumer.close()
    assert hit["telemetry"]["flag"] == "PX_06"
    assert hit["telemetry"]["quarantine_id"]


def test_redriven_record_dedupes_by_business_hash(producer, kafka_config, redis_url):
    import redis as redis_lib
    from vita.persistence import TransitionLedger, compute_payload_hash

    r = redis_lib.from_url(redis_url, decode_responses=True, socket_timeout=2.0)
    ledger = TransitionLedger(r)
    raw_topic = os.environ.get("VITA_INPUT_TOPIC", "vita.raw.events")
    user_id = f"DEDUP_{uuid.uuid4().hex[:8]}"   # uniqueness is the isolation
    event_time = time.time()

    business = {
        "user_id": user_id,
        "current_state": "ACTIVE",
        "target_state": "DORMANT",
        "event_time": event_time,
        "state_entry_time": event_time - 60.0,
        "average_order_value": 250.0,
    }
    producer.produce(raw_topic, key=user_id, value=json.dumps(business))

    envelope = dict(business)
    envelope["_redrive"] = {
        "redrive_id": uuid.uuid4().hex,
        "quarantine_id": "irrelevant",
        "operator": "pytest",
        "reason": "dedup-proof",
        "original_flag": "PX_02",
        "timestamp": time.time(),
    }
    producer.produce(raw_topic, key=user_id,
                     value=json.dumps(envelope),
                     headers={"re-driven": "true"})
    producer.flush(timeout=10)

    expected_hash = compute_payload_hash(business)
    deadline = time.time() + 20
    while time.time() < deadline and not ledger.contains(expected_hash):
        time.sleep(0.25)
    assert ledger.contains(expected_hash), "ledger never received expected hash"

    time.sleep(2.0)
    matching = [e for e in ledger.read_all() if e.payload_hash == expected_hash]
    assert len(matching) == 1, (
        f"expected exactly one entry, got {len(matching)}: "
        f"{[(e.entry_id, e.event_time) for e in matching]}"
    )
