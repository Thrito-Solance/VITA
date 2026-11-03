"""Idempotency store and append-only transition ledger."""
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) 2026 thrito-solance
from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol, Tuple

log = logging.getLogger(__name__)


# =========================================================================
# Errors
# =========================================================================

class PersistenceError(Exception):
    pass

class LedgerWriteError(PersistenceError):
    pass

class IdempotencyError(PersistenceError):
    pass


# =========================================================================
# Backend protocol (redis-py compatible)
# =========================================================================

class Backend(Protocol):
    def set(self, key: str, value: str, *, nx: bool = False, ex: Optional[int] = None) -> bool: ...
    def get(self, key: str) -> Optional[str]: ...
    def delete(self, *keys: str) -> int: ...
    def exists(self, *keys: str) -> int: ...
    def xadd(self, name: str, fields: Dict[str, Any], *,
             maxlen: Optional[int] = None) -> str: ...
    def xrange(self, name: str, start: str = "-", end: str = "+",
               count: Optional[int] = None) -> List[Tuple[str, Dict[str, str]]]: ...


class InMemoryBackend:
    """Thread-safe, TTL-aware, Redis-semantics-compatible."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._kv: Dict[str, Tuple[str, Optional[float]]] = {}
        self._streams: Dict[str, List[Tuple[str, Dict[str, str]]]] = {}
        self._seq: Dict[str, int] = {}

    def _expire(self, key: str) -> None:
        e = self._kv.get(key)
        if e and e[1] is not None and time.time() >= e[1]:
            del self._kv[key]

    def set(self, key, value, *, nx=False, ex=None):
        with self._lock:
            self._expire(key)
            if nx and key in self._kv:
                return False
            self._kv[key] = (value, (time.time() + ex) if ex is not None else None)
            return True

    def get(self, key):
        with self._lock:
            self._expire(key)
            e = self._kv.get(key)
            return e[0] if e else None

    def delete(self, *keys):
        with self._lock:
            n = 0
            for k in keys:
                self._expire(k)
                if k in self._kv:
                    del self._kv[k]; n += 1
            return n

    def exists(self, *keys):
        with self._lock:
            n = 0
            for k in keys:
                self._expire(k)
                if k in self._kv: n += 1
            return n

    def xadd(self, name, fields, *, maxlen=None):
        with self._lock:
            seq = self._seq.get(name, 0) + 1
            self._seq[name] = seq
            eid = f"{int(time.time() * 1000)}-{seq}"
            s = self._streams.setdefault(name, [])
            s.append((eid, {k: str(v) for k, v in fields.items()}))
            if maxlen is not None and len(s) > maxlen:
                self._streams[name] = s[-maxlen:]
            return eid

    def xrange(self, name, start="-", end="+", count=None):
        with self._lock:
            out = []
            for eid, flds in self._streams.get(name, []):
                if count is not None and len(out) >= count: break
                out.append((eid, dict(flds)))
            return out

    def append_once(self, index_key: str, stream_name: str,
                    fields: Dict[str, str]) -> Tuple[str, bool]:
        with self._lock:
            self._expire(index_key)
            existing = self._kv.get(index_key)
            if existing is not None:
                entry_id = json.loads(existing[0]).get("entry_id")
                if entry_id:
                    return entry_id, True
                raise LedgerWriteError("ledger index entry is malformed")

            stream = self._streams.setdefault(stream_name, [])
            for entry_id, stored_fields in stream:
                if stored_fields.get("payload_hash") == fields["payload_hash"]:
                    self._kv[index_key] = (
                        json.dumps({"entry_id": entry_id,
                                    "payload_hash": fields["payload_hash"]}),
                        None,
                    )
                    return entry_id, True

            seq = self._seq.get(stream_name, 0) + 1
            self._seq[stream_name] = seq
            entry_id = fields["entry_id"]
            stream.append((
                f"{int(time.time() * 1000)}-{seq}",
                {key: str(value) for key, value in fields.items()},
            ))
            self._kv[index_key] = (
                json.dumps({"entry_id": entry_id,
                            "payload_hash": fields["payload_hash"]}),
                None,
            )
            return entry_id, False


# =========================================================================
# Payload hash — canonical sha256 of a dict
# =========================================================================

def compute_payload_hash(payload: Dict[str, Any]) -> str:
    canonical = json.dumps(
        payload, sort_keys=True, allow_nan=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# =========================================================================
# Ledger entry
# =========================================================================

@dataclass(frozen=True)
class LedgerEntry:
    user_id: str
    event_time: float
    from_state: str
    to_state: str
    rule_version: str
    computed_probability: float
    why_rationale: str
    action_key: str
    payload_hash: str
    entry_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    ledger_timestamp: float = field(default_factory=time.time)

    def to_stream_fields(self) -> Dict[str, str]:
        return {
            "entry_id": self.entry_id,
            "user_id": self.user_id,
            "event_time": repr(self.event_time),
            "from_state": self.from_state,
            "to_state": self.to_state,
            "rule_version": self.rule_version,
            "computed_probability": repr(self.computed_probability),
            "why_rationale": self.why_rationale,
            "action_key": self.action_key,
            "payload_hash": self.payload_hash,
            "ledger_timestamp": repr(self.ledger_timestamp),
        }

    @classmethod
    def from_stream_fields(cls, f: Dict[str, str]) -> "LedgerEntry":
        return cls(
            entry_id=f["entry_id"],
            user_id=f["user_id"],
            event_time=float(f["event_time"]),
            from_state=f["from_state"],
            to_state=f["to_state"],
            rule_version=f["rule_version"],
            computed_probability=float(f["computed_probability"]),
            why_rationale=f["why_rationale"],
            action_key=f["action_key"],
            payload_hash=f["payload_hash"],
            ledger_timestamp=float(f["ledger_timestamp"]),
        )


@dataclass(frozen=True)
class LedgerReceipt:
    entry_id: str
    payload_hash: str
    duplicate: bool


# =========================================================================
# Idempotency store
# =========================================================================

class IdempotencyStore:
    DEFAULT_TTL = 86_400

    def __init__(self, backend: Backend, ttl_seconds: int = DEFAULT_TTL) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self._backend = backend
        self._ttl = ttl_seconds

    def try_reserve(self, key: str) -> bool:
        try:
            return bool(self._backend.set(key, "1", nx=True, ex=self._ttl))
        except Exception as exc:
            raise IdempotencyError(f"reservation failed: {exc}") from exc

    def release(self, key: str) -> None:
        try:
            self._backend.delete(key)
        except Exception as exc:
            log.error("idempotency release failed for %s: %s", key, exc)
            raise IdempotencyError(f"release failed: {exc}") from exc

    def is_reserved(self, key: str) -> bool:
        try:
            return self._backend.exists(key) > 0
        except Exception as exc:
            raise IdempotencyError(f"exists failed: {exc}") from exc


# =========================================================================
# Transition ledger — idempotent append by payload_hash
# =========================================================================

class TransitionLedger:
    STREAM_NAME = "vita:ledger:transitions"
    INDEX_PREFIX = "vita:ledger:hash:"
    _APPEND_SCRIPT = """
local existing = redis.call('GET', KEYS[1])
if existing then
    local index = cjson.decode(existing)
    return {1, index['entry_id']}
end
local fields = cjson.decode(ARGV[1])
local names = {
    'entry_id', 'user_id', 'event_time', 'from_state', 'to_state',
    'rule_version', 'computed_probability', 'why_rationale', 'action_key',
    'payload_hash', 'ledger_timestamp'
}
local args = {'*'}
for _, name in ipairs(names) do
    table.insert(args, name)
    table.insert(args, fields[name])
end
redis.call('XADD', KEYS[2], unpack(args))
redis.call('SET', KEYS[1], cjson.encode({
    entry_id = fields['entry_id'],
    payload_hash = fields['payload_hash']
}))
return {0, fields['entry_id']}
"""

    def __init__(self, backend: Backend, stream_name: str = STREAM_NAME) -> None:
        self._backend = backend
        self._stream_name = stream_name

    def _index_key(self, payload_hash: str) -> str:
        return f"{self.INDEX_PREFIX}{payload_hash}"

    def contains(self, payload_hash: str) -> bool:
        try:
            return self._backend.exists(self._index_key(payload_hash)) > 0
        except Exception as exc:
            raise LedgerWriteError(f"index exists failed: {exc}") from exc

    def append(self, entry: LedgerEntry) -> LedgerReceipt:
        index_key = self._index_key(entry.payload_hash)
        try:
            existing = self._backend.get(index_key)
        except Exception as exc:
            raise LedgerWriteError(f"index read failed: {exc}") from exc
        if existing is not None:
            try:
                existing_id = json.loads(existing).get("entry_id")
            except (json.JSONDecodeError, AttributeError) as exc:
                raise LedgerWriteError(f"malformed ledger index: {exc}") from exc
            if not existing_id:
                raise LedgerWriteError("ledger index has no entry_id")
            return LedgerReceipt(existing_id, entry.payload_hash, True)

        fields = entry.to_stream_fields()
        memory_append = getattr(self._backend, "append_once", None)
        if callable(memory_append):
            try:
                entry_id, duplicate = memory_append(index_key, self._stream_name, fields)
            except Exception as exc:
                raise LedgerWriteError(f"atomic in-memory append failed: {exc}") from exc
            return LedgerReceipt(entry_id, entry.payload_hash, duplicate)

        eval_script = getattr(self._backend, "eval", None)
        if not callable(eval_script):
            raise LedgerWriteError("backend does not support atomic ledger append")

        try:
            result = eval_script(
                self._APPEND_SCRIPT,
                2,
                index_key,
                self._stream_name,
                json.dumps(fields, sort_keys=True, separators=(",", ":")),
            )
        except Exception as exc:
            try:
                recovered = next(
                    (item for item in self.read_all()
                     if item.payload_hash == entry.payload_hash),
                    None,
                )
                if recovered is not None:
                    self._backend.set(
                        index_key,
                        json.dumps({"entry_id": recovered.entry_id,
                                    "payload_hash": recovered.payload_hash}),
                        nx=True,
                    )
                    log.warning(
                        "recovered ledger index for already-appended hash %s",
                        entry.payload_hash,
                    )
                    return LedgerReceipt(recovered.entry_id,
                                         entry.payload_hash, True)
            except Exception as recovery_error:
                raise LedgerWriteError(
                    f"ledger append failed: {exc}; recovery failed: {recovery_error}"
                ) from exc
            raise LedgerWriteError(f"atomic ledger append failed: {exc}") from exc

        if not isinstance(result, (list, tuple)) or len(result) != 2:
            raise LedgerWriteError("atomic ledger append returned an invalid result")
        try:
            duplicate = int(result[0]) == 1
            entry_id = result[1]
            if isinstance(entry_id, bytes):
                entry_id = entry_id.decode("utf-8")
            if not isinstance(entry_id, str) or not entry_id:
                raise ValueError("empty entry_id")
        except (TypeError, ValueError, UnicodeDecodeError) as exc:
            raise LedgerWriteError(f"invalid atomic append result: {exc}") from exc
        return LedgerReceipt(entry_id, entry.payload_hash, duplicate)

    def read_all(self) -> List[LedgerEntry]:
        try:
            raw = self._backend.xrange(self._stream_name)
        except Exception as exc:
            raise LedgerWriteError(f"stream read failed: {exc}") from exc
        out: List[LedgerEntry] = []
        for eid, f in raw:
            try:
                out.append(LedgerEntry.from_stream_fields(f))
            except (KeyError, ValueError, TypeError) as exc:
                raise LedgerWriteError(
                    f"malformed ledger entry {eid}: {exc}"
                ) from exc
        return out
