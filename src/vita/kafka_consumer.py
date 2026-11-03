"""VITA streaming processor. Fail-closed on ledger write; quarantine on any anomaly."""
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) 2026 thrito-solance
# If a modified version supports remote network interaction under AGPL-3.0 section 13,
# offer its Corresponding Source; see README.md for this project's notice.
from __future__ import annotations

import json
import logging
import os
import signal
import time

from confluent_kafka import Consumer, Producer, KafkaError

from vita.core import (
    ContractViolation, IntentPayload, NeuroSymbolicTensorCore, TemporalKernel,
)
from vita.persistence import (
    LedgerEntry, LedgerWriteError, TransitionLedger,
)
from vita.redrive import QuarantineStore, compute_business_hash
from vita.schema import validate_and_derive

log = logging.getLogger("vita.kafka")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


class OutputDeliveryError(RuntimeError):
    """Produced Kafka records were not confirmed before input offset commit."""


def _build_engine() -> NeuroSymbolicTensorCore:
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
    engine.register_transition_logic("DORMANT", "EMERGING_VIP", dormant_to_vip)
    return engine


class VITAStreamingProcessor:
    def __init__(self) -> None:
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
        self.input   = os.environ.get("VITA_INPUT_TOPIC", "vita.raw.events")
        self.output  = os.environ.get("VITA_OUTPUT_TOPIC", "vita.processed.trajectories")
        self.actions = os.environ.get("VITA_ACTION_TOPIC", "vita.actions")
        self.dlq     = os.environ.get("VITA_DLQ_TOPIC", "vita.paradox.quarantine")
        group        = os.environ.get("VITA_GROUP_ID", "vita-processors-v1")

        import redis as redis_lib
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        self._redis = redis_lib.from_url(redis_url, decode_responses=True, socket_timeout=2.0)

        self.engine = _build_engine()
        self.quarantine = QuarantineStore(self._redis)
        self.ledger = TransitionLedger(self._redis)
        self._delivery_errors = []

        self.running = True
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "max.poll.interval.ms": 300_000,
            "session.timeout.ms": 45_000,
        })
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "max.in.flight.requests.per.connection": 5,
        })

    def _report(self, err, msg):
        if err is not None:
            log.error("delivery failed to %s: %s", msg.topic(), err)
            self._delivery_errors.append(str(err))

    def _emit(self, topic, key, value, headers):
        self.producer.produce(
            topic=topic,
            key=(key or "").encode(),
            value=json.dumps(value, allow_nan=True).encode(),
            headers={k: str(v).encode() for k, v in headers.items()},
            on_delivery=self._report,
        )

    def _flush_outputs(self) -> None:
        remaining = self.producer.flush(timeout=30.0)
        errors, self._delivery_errors = self._delivery_errors, []
        if remaining or errors:
            raise OutputDeliveryError(
                f"Kafka output delivery incomplete: {remaining} pending, "
                f"errors={errors}"
            )

    def _emit_dlq(self, user_id, quarantine_id, flag, rationale, raw):
        self._emit(self.dlq, quarantine_id, {
            "status": "PARADOX_QUARANTINE_HANDOFF",
            "telemetry": {
                "flag": flag,
                "system_rationale": rationale,
                "timestamp": time.time(),
                "quarantine_id": quarantine_id,
                "offending_payload": raw,
            },
        }, {"flag": flag, "user-id": user_id or "UNKNOWN",
            "quarantine-id": quarantine_id})

    def _quarantine_both(self, raw, flag, rationale):
        uid = raw.get("user_id") if isinstance(raw, dict) else None
        record = self.quarantine.quarantine(
            raw if isinstance(raw, dict) else {"raw": raw}, flag, rationale
        )
        self._emit_dlq(uid, record.quarantine_id, flag, rationale, raw)
        self._flush_outputs()

    def _handle(self, msg) -> None:
        self._delivery_errors.clear()
        raw_bytes = msg.value()
        try:
            raw = json.loads(raw_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            self._quarantine_both(
                {"user_id": "UNKNOWN", "_undecodable": raw_bytes[:64].hex()},
                "PX_00", f"undecodable payload: {exc}",
            )
            return

        if not isinstance(raw, dict):
            self._quarantine_both(
                {"user_id": "UNKNOWN", "_type": type(raw).__name__},
                "PX_04", f"payload must be object, got {type(raw).__name__}",
            )
            return

        try:
            record = validate_and_derive(raw)
        except ContractViolation as exc:
            self._quarantine_both(raw, exc.code, str(exc))
            return

        try:
            result = self.engine.evaluate(record)
        except ContractViolation as exc:
            self._quarantine_both(record, exc.code, str(exc))
            return

        if result["status"] != "PROCESSED_SUCCESSFULLY":
            t = result["telemetry"]
            self._quarantine_both(record, t["flag"], t["system_rationale"])
            return

        intent = result["intent"]
        entry = LedgerEntry(
            user_id=record["user_id"],
            event_time=record["event_time"],
            from_state=record["current_state"],
            to_state=record["target_state"],
            rule_version=intent.rule_version,
            computed_probability=result["metadata"]["computed_probability"],
            why_rationale=intent.why_rationale,
            action_key=intent.action_key,
            payload_hash=compute_business_hash(raw),
        )
        try:
            self.ledger.append(entry)
        except LedgerWriteError as exc:
            log.error("ledger write failed — fail-closed, offset not committed: %s", exc)
            raise

        self._emit(self.output, entry.payload_hash, {
            "status": "PROCESSED_SUCCESSFULLY",
            "metadata": result["metadata"],
        }, {
            "why": intent.why_rationale,
            "what": intent.action_key,
            "rule-version": intent.rule_version,
            "business-hash": entry.payload_hash,
        })

        self._emit(self.actions, entry.payload_hash, {
            "action_key": intent.action_key,
            "user_id": record["user_id"],
            "why": intent.why_rationale,
            "rule_version": intent.rule_version,
            "business_hash": entry.payload_hash,
            "context": {"from": record["current_state"],
                        "to": record["target_state"]},
        }, {
            "action": intent.action_key,
            "business-hash": entry.payload_hash,
        })

        self._flush_outputs()

    def run(self) -> None:
        self.consumer.subscribe([self.input])
        log.info("subscribed to %s", self.input)
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    log.error("consumer error: %s", msg.error())
                    if msg.error().fatal():
                        break
                    continue
                try:
                    self._handle(msg)
                except Exception:
                    log.exception(
                        "message handling failed; stopping to preserve offset order"
                    )
                    break
                try:
                    committed = self.consumer.commit(message=msg, asynchronous=False)
                    if committed:
                        errors = [part.error for part in committed if part.error]
                        if errors:
                            raise RuntimeError(f"offset commit failed: {errors}")
                except Exception:
                    log.exception(
                        "offset commit failed; stopping to preserve offset order"
                    )
                    break
        finally:
            self.consumer.close()
            self.producer.flush(timeout=30)
            log.info("shutdown complete")

    def stop(self, *_):
        self.running = False


def main() -> None:
    p = VITAStreamingProcessor()
    signal.signal(signal.SIGINT, p.stop)
    signal.signal(signal.SIGTERM, p.stop)
    p.run()


if __name__ == "__main__":
    main()
