"""Paradox quarantine (compressed index + isolated payload) and safe re-drive."""
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) 2026 thrito-solance
from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Protocol

from vita.core import ContractViolation, NeuroSymbolicTensorCore
from vita.mcda_core import InvalidScoreBoundaryError
from vita.persistence import Backend, compute_payload_hash
from vita.schema import validate_and_derive

log = logging.getLogger(__name__)


class QuarantineError(Exception): ...
class QuarantineNotFound(QuarantineError): ...
class PayloadAccessError(QuarantineError): ...
class PayloadIntegrityError(QuarantineError): ...
class ReDriveRejected(QuarantineError): ...
class ReDriveBusy(QuarantineError): ...
class ReDriveAlreadyDone(QuarantineError): ...


class Keys:
    INDEX_STREAM         = "vita:quarantine:index"
    PAYLOAD_HASH         = "vita:quarantine:payload"
    REDRIVE_AUDIT_STREAM = "vita:redrive:audit"
    REDRIVE_DONE         = "vita:redrive:done"
    REDRIVE_LOCK         = "vita:redrive:lock"
    REDRIVE_RECEIPT      = "vita:redrive:receipt"
    REDRIVE_IDEM         = "vita:idem:redrive"

    @classmethod
    def payload(cls, qid): return f"{cls.PAYLOAD_HASH}:{qid}"
    @classmethod
    def done(cls, qid):    return f"{cls.REDRIVE_DONE}:{qid}"
    @classmethod
    def lock(cls, qid):    return f"{cls.REDRIVE_LOCK}:{qid}"
    @classmethod
    def receipt(cls, rid): return f"{cls.REDRIVE_RECEIPT}:{rid}"
    @classmethod
    def idem(cls, rid):    return f"{cls.REDRIVE_IDEM}:{rid}"


TRUNCATED_HASH_LEN = 16
INTERNAL_PREFIX = "_"


def compute_business_hash(payload: Dict[str, Any]) -> str:
    business = {k: v for k, v in payload.items() if not k.startswith(INTERNAL_PREFIX)}
    return compute_payload_hash(business)


def idempotency_key_for(payload: Dict[str, Any]) -> str:
    redrive = payload.get("_redrive")
    if isinstance(redrive, dict) and redrive.get("redrive_id"):
        return Keys.idem(redrive["redrive_id"])
    return f"vita:idem:{payload['user_id']}:{payload['event_time']}"


@dataclass(frozen=True)
class QuarantineRecord:
    quarantine_id: str
    flag: str
    rationale: str
    timestamp: float
    truncated_payload_hash: str
    user_id: Optional[str]
    from_state: Optional[str]
    to_state: Optional[str]
    rule_version: Optional[str]
    redrive_status: str


class QuarantineStore:
    DEFAULT_PAYLOAD_TTL = 604_800

    def __init__(self, backend: Backend, payload_ttl: int = DEFAULT_PAYLOAD_TTL) -> None:
        if (
            not isinstance(payload_ttl, int)
            or isinstance(payload_ttl, bool)
            or payload_ttl <= 0
        ):
            raise ValueError("payload_ttl must be positive")
        self._backend = backend
        self._payload_ttl = payload_ttl

    def quarantine(self, payload: Dict[str, Any], flag: str,
                   rationale: str) -> QuarantineRecord:
        if not isinstance(payload, dict):
            raise QuarantineError("payload must be an object")
        if not isinstance(flag, str) or not flag.startswith("PX_"):
            raise QuarantineError(f"invalid flag: {flag!r}")
        if not isinstance(rationale, str) or not rationale.strip():
            raise QuarantineError("rationale must be non-empty")

        qid = uuid.uuid4().hex
        truncated = compute_payload_hash(payload)[:TRUNCATED_HASH_LEN]

        try:
            ok = self._backend.set(Keys.payload(qid),
                                   json.dumps(payload, sort_keys=True, default=str),
                                   nx=True, ex=self._payload_ttl)
            if not ok:
                raise QuarantineError(f"payload slot collision: {qid}")
        except QuarantineError:
            raise
        except Exception as exc:
            raise QuarantineError(f"payload write failed: {exc}") from exc

        ts = time.time()
        fields = {
            "quarantine_id": qid,
            "flag": flag,
            "rationale": rationale,
            "timestamp": repr(ts),
            "truncated_payload_hash": truncated,
            "user_id": str(payload.get("user_id", "") or ""),
            "from_state": str(payload.get("current_state", "") or ""),
            "to_state": str(payload.get("target_state", "") or ""),
            "rule_version": str(payload.get("rule_version", "") or ""),
        }
        try:
            self._backend.xadd(Keys.INDEX_STREAM, fields)
        except Exception as exc:
            log.error("index write failed for %s; payload orphaned (ttl=%ds): %s",
                      qid, self._payload_ttl, exc)
            raise QuarantineError(f"index write failed: {exc}") from exc

        return QuarantineRecord(
            quarantine_id=qid, flag=flag, rationale=rationale, timestamp=ts,
            truncated_payload_hash=truncated,
            user_id=fields["user_id"] or None,
            from_state=fields["from_state"] or None,
            to_state=fields["to_state"] or None,
            rule_version=fields["rule_version"] or None,
            redrive_status="open",
        )

    def _status(self, qid: str) -> str:
        try:
            if self._backend.exists(Keys.done(qid)) > 0:
                return "re_driven"
            if self._backend.exists(Keys.lock(qid)) > 0:
                return "in_progress"
        except Exception as exc:
            raise QuarantineError(f"status lookup failed for {qid}: {exc}") from exc
        return "open"

    def _record_from_fields(self, f: Dict[str, str]) -> QuarantineRecord:
        qid = f["quarantine_id"]
        return QuarantineRecord(
            quarantine_id=qid,
            flag=f["flag"],
            rationale=f["rationale"],
            timestamp=float(f["timestamp"]),
            truncated_payload_hash=f["truncated_payload_hash"],
            user_id=f.get("user_id") or None,
            from_state=f.get("from_state") or None,
            to_state=f.get("to_state") or None,
            rule_version=f.get("rule_version") or None,
            redrive_status=self._status(qid),
        )

    def inspect(self, flag: Optional[str] = None, user_id: Optional[str] = None,
                since: Optional[float] = None,
                limit: int = 100) -> List[QuarantineRecord]:
        if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
            raise ValueError("limit must be positive")
        try:
            raw = self._backend.xrange(Keys.INDEX_STREAM)
        except Exception as exc:
            raise QuarantineError(f"index read failed: {exc}") from exc
        out: List[QuarantineRecord] = []
        for _eid, f in raw:
            if flag is not None and f.get("flag") != flag: continue
            if user_id is not None and f.get("user_id") != user_id: continue
            if since is not None and float(f.get("timestamp", 0)) < since: continue
            out.append(self._record_from_fields(f))
            if len(out) >= limit:
                break
        return out

    def find_by_id(self, qid: str) -> QuarantineRecord:
        try:
            entries = self._backend.xrange(Keys.INDEX_STREAM)
        except Exception as exc:
            raise QuarantineError(f"index read failed: {exc}") from exc
        for _eid, f in entries:
            if f.get("quarantine_id") == qid:
                return self._record_from_fields(f)
        raise QuarantineNotFound(qid)

    def load_payload(self, qid: str) -> Dict[str, Any]:
        try:
            blob = self._backend.get(Keys.payload(qid))
        except Exception as exc:
            raise PayloadAccessError(f"payload read failed: {exc}") from exc
        if blob is None:
            raise PayloadAccessError(f"payload not found or TTL-expired: {qid}")
        try:
            payload = json.loads(blob)
        except json.JSONDecodeError as exc:
            raise PayloadAccessError(f"payload corrupt: {qid}: {exc}") from exc
        if not isinstance(payload, dict):
            raise PayloadAccessError(f"payload is not an object: {qid}")

        record = self.find_by_id(qid)
        expected = compute_payload_hash(payload)[:TRUNCATED_HASH_LEN]
        if expected != record.truncated_payload_hash:
            raise PayloadIntegrityError(
                f"payload/index hash mismatch for {qid}: "
                f"index={record.truncated_payload_hash} payload={expected}"
            )
        return payload


@dataclass(frozen=True)
class ReDriveReceipt:
    redrive_id: str
    quarantine_id: str
    injected_at: float
    business_hash: str

    def _serialize(self) -> str:
        return json.dumps({
            "redrive_id": self.redrive_id,
            "quarantine_id": self.quarantine_id,
            "injected_at": self.injected_at,
            "business_hash": self.business_hash,
        })

    @classmethod
    def _deserialize(cls, s: str) -> "ReDriveReceipt":
        d = json.loads(s)
        return cls(d["redrive_id"], d["quarantine_id"],
                   float(d["injected_at"]), d["business_hash"])


class Injector(Protocol):
    def __call__(self, topic: str, key: str,
                 payload: Dict[str, Any], headers: Dict[str, str]) -> None: ...


class ReDrivePipeline:
    DEFAULT_LOCK_TTL = 300
    DEFAULT_IDEM_TTL = 86_400

    def __init__(self, quarantine: QuarantineStore,
                 engine: NeuroSymbolicTensorCore, backend: Backend,
                 injector: Injector, input_topic: str,
                 lock_ttl: int = DEFAULT_LOCK_TTL,
                 idem_ttl: int = DEFAULT_IDEM_TTL) -> None:
        if lock_ttl <= 0 or idem_ttl <= 0:
            raise ValueError("re-drive TTL values must be positive")
        self._quarantine = quarantine
        self._engine = engine
        self._backend = backend
        self._injector = injector
        self._topic = input_topic
        self._lock_ttl = lock_ttl
        self._idem_ttl = idem_ttl

    def redrive(self, quarantine_id: str, corrected_payload: Dict[str, Any],
                operator: str, reason: str,
                redrive_id: Optional[str] = None) -> ReDriveReceipt:
        if not isinstance(operator, str) or not operator.strip():
            raise ReDriveRejected("operator must be non-empty")
        if not isinstance(reason, str) or not reason.strip():
            raise ReDriveRejected("reason must be non-empty")
        if not isinstance(corrected_payload, dict):
            raise ReDriveRejected("corrected_payload must be an object")
        if redrive_id is not None and (
            not isinstance(redrive_id, str) or not redrive_id.strip()
        ):
            raise ReDriveRejected("redrive_id must be a non-empty string")

        corrected_business = {
            k: v for k, v in corrected_payload.items()
            if not k.startswith(INTERNAL_PREFIX)
        }
        try:
            validated = validate_and_derive(corrected_business)
        except InvalidScoreBoundaryError as exc:
            raise ReDriveRejected(f"schema gate: {exc.code}: {exc}") from exc
        except ContractViolation as exc:
            raise ReDriveRejected(f"schema gate: {exc.code}: {exc}") from exc
        business_hash = compute_business_hash(validated)

        rid = redrive_id or uuid.uuid4().hex

        # 1. API idempotency — same redrive_id → cached receipt.
        try:
            cached = self._backend.get(Keys.receipt(rid))
        except Exception as exc:
            raise QuarantineError(f"receipt read failed: {exc}") from exc
        if cached is not None:
            receipt = ReDriveReceipt._deserialize(cached)
            if (receipt.quarantine_id != quarantine_id
                    or receipt.business_hash != business_hash):
                raise ReDriveRejected(
                    "redrive_id is already bound to a different request"
                )
            return receipt

        # 2. Already re-driven?
        original = self._quarantine.find_by_id(quarantine_id)
        try:
            if self._backend.exists(Keys.done(quarantine_id)) > 0:
                raise ReDriveAlreadyDone(f"{quarantine_id} already re-driven")
        except ReDriveAlreadyDone:
            raise
        except Exception as exc:
            raise QuarantineError(f"done check failed: {exc}") from exc

        # 3. Claim short-TTL lock.
        try:
            claimed = self._backend.set(Keys.lock(quarantine_id), rid,
                                        nx=True, ex=self._lock_ttl)
        except Exception as exc:
            raise QuarantineError(f"lock claim failed: {exc}") from exc
        if not claimed:
            raise ReDriveBusy(f"another re-drive in flight for {quarantine_id}")

        try:
            # 4. Original payload integrity check.
            self._quarantine.load_payload(quarantine_id)

            # 5. Engine dry-run — pure, no emit.
            try:
                dry = self._engine.evaluate(validated)
            except ContractViolation as exc:
                raise ReDriveRejected(f"engine gate: {exc.code}: {exc}") from exc
            if dry.get("status") != "PROCESSED_SUCCESSFULLY":
                flag = dry.get("telemetry", {}).get("flag", "PX_UNKNOWN")
                why = dry.get("telemetry", {}).get("system_rationale", "")
                raise ReDriveRejected(
                    f"corrected payload still quarantines: {flag}: {why}"
                )

            # 6. Own idempotency namespace — never collides with original.
            try:
                idem_ok = self._backend.set(Keys.idem(rid), "1",
                                            nx=True, ex=self._idem_ttl)
            except Exception as exc:
                raise QuarantineError(f"idempotency claim failed: {exc}") from exc
            if not idem_ok:
                raise ReDriveBusy(f"re-drive {rid} already claimed")

            # 7. Envelope + inject.
            envelope: Dict[str, Any] = dict(validated)
            envelope["_redrive"] = {
                "redrive_id": rid,
                "quarantine_id": quarantine_id,
                "operator": operator,
                "reason": reason,
                "original_flag": original.flag,
                "business_hash": business_hash,
                "timestamp": time.time(),
            }
            headers = {
                "re-driven": "true",
                "redrive-id": rid,
                "quarantine-id": quarantine_id,
                "operator": operator,
                "business-hash": business_hash,
            }
            try:
                self._injector(self._topic, str(validated["user_id"]),
                               envelope, headers)
            except Exception as exc:
                try:
                    self._backend.delete(Keys.idem(rid))
                except Exception:
                    log.exception("idempotency release failed for %s", rid)
                raise QuarantineError(f"injection failed: {exc}") from exc

            injected_at = time.time()
            try:
                self._backend.xadd(Keys.REDRIVE_AUDIT_STREAM, {
                    "redrive_id": rid,
                    "quarantine_id": quarantine_id,
                    "operator": operator,
                    "reason": reason,
                    "original_flag": original.flag,
                    "business_hash": business_hash,
                    "timestamp": repr(injected_at),
                })
            except Exception as exc:
                try:
                    self._backend.delete(Keys.idem(rid))
                except Exception:
                    log.exception("idempotency release failed after audit error for %s", rid)
                raise QuarantineError(
                    f"record was injected but audit append failed for {rid}; "
                    "delivery outcome is uncertain"
                ) from exc

            # 8. Commit markers only after the audit record is durable.
            receipt = ReDriveReceipt(rid, quarantine_id, injected_at, business_hash)
            self._backend.set(Keys.receipt(rid), receipt._serialize(), ex=self._idem_ttl)
            self._backend.set(Keys.done(quarantine_id), rid)

            return receipt
        finally:
            try:
                self._backend.delete(Keys.lock(quarantine_id))
            except Exception as exc:
                log.error("lock release failed for %s: %s", quarantine_id, exc)
