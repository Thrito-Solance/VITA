# VITA — Vectorized Intent Tracking Architecture

VITA is a Python event-processing reference implementation for explicit
intent/action metadata, validated state transitions, a Redis-backed transition
ledger, quarantine inspection, and guarded re-drive into Kafka.

The supplied notes described both a pure state engine and a distributed
processor. This project keeps those concerns separate: `core.py` has no I/O;
the Kafka, Redis, and operator surfaces are adapters. The sparse transition map
is a lookup structure, not a requirement to avoid ordinary control flow.

## Status and guarantees

This is a reference implementation, not a claim of exactly-once processing.
Redis and Kafka do not share a transaction. Ledger writes are deduplicated by
business hash; Kafka output is at-least-once across crashes and consumers must
deduplicate using the stable `business-hash` header. Verify delivery before
committing input offsets. Re-drive also needs downstream deduplication by
`redrive-id`/`business-hash`.

The demo Compose stack is for local development only. It has no production
authentication, TLS, replication, retention policy, or disaster-recovery
configuration. Do not expose it to untrusted networks or use its `clean` target
against valuable data.

## Run locally

Requirements: Python 3.11+, Docker Compose v2 for integration, and permission
to download the declared packages/container images.

```powershell
python -m pip install -e ".[dev]"
python -m pytest -q tests/test_vita_resilience.py
```

For the local Kafka/Redis demo:

```powershell
docker compose up -d --build
docker compose run --rm processor-dev python -m scripts.seed
docker compose run --rm processor-dev python -m vita.cli inspect
docker compose run --rm processor-dev pytest -q -m integration tests/test_integration.py
docker compose down
```

## Architecture

- `src/vita/core.py` — intent metadata, validated temporal kernel, sparse transition rules
- `src/vita/mcda_core.py` — pure, bounded Beta PDF/CDF model for intent scores
- `src/vita/schema.py` — input validation, MCDA score metadata, and server-side elapsed-time derivation
- `src/vita/persistence.py` — Redis/in-memory backend, idempotency primitives, transition ledger
- `src/vita/redrive.py` — quarantine records, payload integrity checks, guarded re-drive
- `src/vita/kafka_consumer.py` — Kafka adapter; external effects are not atomic with Redis
- `tests/` — in-memory resilience tests and opt-in integration checks

When an input includes `intent_score`, it must be a finite number in `[0, 10]`.
The schema adds `intent_score_normalized`, `intent_score_density`, and
`intent_score_percentile`; the percentile is the CDF fraction in `[0, 1]`.
Each derived numeric value is rounded to four decimal places before it is
returned. Inputs without `intent_score` retain the existing schema behavior.

## Licensing

This project is licensed under GNU AGPL version 3 or (at your option) any later
version. See [LICENSE](./LICENSE). Copyright attribution in generated source
is `thrito-solance`, as supplied for this project; authorship and ownership
have not been independently verified.

Under AGPL-3.0 section 13, a modified version that supports remote network
interaction must provide its users an opportunity to receive the version's
Corresponding Source. The exact obligation depends on the distribution and
modification facts; this notice is not legal advice.

No commercial license or proprietary exception is granted by this repository.
See [COMMERCIAL-LICENSING.md](./COMMERCIAL-LICENSING.md) for the neutral
rights-holder inquiry notice; no commercial grant is made here. Contact
details and terms are not provided.
