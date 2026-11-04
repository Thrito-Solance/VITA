# VITA synthesis and architectural review

## 0. Runtime manifest

### Auditor runtime

| Capability | Status | Evidence / boundary |
|---|---|---|
| Workspace read/write | VERIFIED | Read the four supplied `.txt` documents and created the `vita` project. |
| Code execution | VERIFIED | Python 3.14.2 and pytest 9.1.1 ran locally. |
| Network access | VERIFIED, NARROW | `curl.exe` fetched the official GNU AGPL-3.0 text. This does not establish general network reachability. |
| Persistence | VERIFIED | Project files persisted under this workspace. The workspace is not a Git repository. |
| Permissions | VERIFIED, NARROW | Workspace file creation/editing succeeded; no production credentials or deployment permissions were tested. |
| Docker / Kafka / Redis | UNAVAILABLE IN THIS RUN | Docker is not on PATH; `redis` and `confluent_kafka` Python packages are not installed. No service integration was run. |
| Sandboxing | UNRESOLVED | No target sandbox or isolation guarantees were supplied. |
| External integrations | UNRESOLVED | No production Kafka, Redis, or downstream consumer was reachable or tested. |
| Destructive test permissions | RESTRICTED | No `make clean`, database flush, or live-service mutation was performed. Redis tests now use a unique test-key namespace instead of `FLUSHDB`. |

### Target runtime

[EVIDENCE] The generated package declares Python `>=3.11`, Redis, and
Confluent Kafka; the supplied deployment notes specify Python 3.12, Redis 7,
and Kafka 3.8.1. These are declared targets, not verified deployed versions.
[UNKNOWN] Production topology, data classification, authentication, retention,
availability objectives, and operator permissions were not supplied.

The prompt defines phases P0–P5 but not P6. [UNKNOWN] No P6 behavior is inferred.

## 1. Structural baselines and licensing boundaries

### Intended scope

[EVIDENCE] The source set describes a validated event-to-transition pipeline:
schema gate, sparse state-edge lookup, intent metadata, Redis transition ledger,
quarantine inspection, and guarded re-drive into Kafka. The Python package in
this folder is the selected baseline; the single-file Rust port is treated as
an earlier prototype and critique, not as the production implementation.

### Invariants

1. Validate payload shape and finite time/value inputs before rule evaluation.
2. A transition is eligible only when its source and destination states and
   edge are registered.
3. The core returns a decision; external effects belong to adapters.
4. Persist the transition before reporting success; do not commit a Kafka input
   offset until required output delivery is confirmed.
5. Ledger deduplication must not strand an event on a crash between a
   reservation and the ledger append.
6. Re-drive must use the same schema and engine gates, bind an idempotency
   identifier to one request, and surface uncertain external outcomes.
7. State-transition probability is bounded and finite. The current engine
   reports it; it does not use it to make stochastic decisions.

### Licensing boundary

[EVIDENCE] The requester selected `AGPL-3.0-or-later`, supplied
`thrito-solance` as the attribution, and requested a neutral commercial inquiry
notice. These values are reproduced in source SPDX headers, package metadata,
the complete `LICENSE`, and [COMMERCIAL-LICENSING.md](./COMMERCIAL-LICENSING.md).
[UNKNOWN] Copyright title and authority were not independently verified; no
contact address or commercial terms were supplied.

[EVIDENCE] The commercial notice grants no proprietary exception and promises
no commercial license. Any such grant requires a separate written agreement
from the verified rights holder. [INFERENCE] A dual-licensing arrangement
cannot be made effective merely by adding a notice; the licensor must control
the relevant rights.

[EVIDENCE] The README and Kafka adapter call out AGPL-3.0 section 13 for a
modified version that supports remote network interaction. The obligation is
described conditionally; this project makes no legal determination about a
particular deployment. The official license text was fetched from
<https://www.gnu.org/licenses/agpl-3.0.txt>.

## 2. Ranked failure analysis and concrete falsifiers

Ranking prioritizes invalid-success/data-loss consequences, then whether a
failure can permanently suppress a valid retry, then product-boundary
uncertainty. “CONFIRMED” refers to the supplied source behavior, not to an
observed production incident. Fixes listed here are present in this generated
project; runtime verification is stated separately.

| # | Severity | Finding and status | Confidence | Causal path | Concrete falsifier / validation |
|---|---|---|---|---|---|
| 1 | CRITICAL | **[CONFIRMED; repaired] Non-finite input could pass direct engine calls.** In the supplied Python core, `t < 0` does not reject NaN; an edge rule not using the temporal kernel could return a finite probability and reach success. The Rust prototype records the same NaN comparison failure. | [CONFIDENCE: High] Directly visible in source and covered by regression tests. | Caller bypasses schema gate → NaN time/AOV reaches core → comparisons fail open → processed result. | **EXECUTED:** direct core tests exercise NaN time and NaN VIP AOV; both quarantine. A non-finite kernel input test also executes. |
| 2 | CRITICAL | **[CONFIRMED; repaired, integration unverified] Kafka output was asynchronous while the input offset could be committed.** Delivery errors were only logged and callbacks could occur after commit; the loop also continued after a handler/commit error, risking later same-partition commits advancing past the failed message. | [CONFIDENCE: High] Original control flow is directly visible; live broker behavior was not tested. | Ledger append → enqueue output → broker delivery/offset commit fails → later offset is committed or earlier input is not replayed → output absent. | **PROCESSED, not executed:** inject producer delivery and offset-commit failures; assert no offset commit for the failed message and the processor stops before handling a later same-partition message. Kafka is unavailable in this runtime. |
| 3 | HIGH | **[CONFIRMED; repaired, Redis unverified] Idempotency reservation preceded durable ledger append.** A crash could leave the 24-hour key reserved; redelivery treated it as a duplicate and committed without a ledger entry. | [CONFIDENCE: High] Reservation/commit sequence is directly visible in source. | SET-NX reservation → process crash → retry sees reservation → message acknowledged without ledger append. | **PROCESSED, not executed:** fault-inject process loss after reservation and verify the retry still reaches the ledger. The consumer no longer uses this reservation gate; atomic ledger append is the deduplication authority. |
| 4 | HIGH | **[CONFIRMED; repair implemented] Ledger index and stream were changed in separate client operations.** A crash between claim and append could leave a permanent false duplicate. | [CONFIDENCE: High] Separate writes and their order are directly visible in source. | Redis index claim → crash before XADD → retry sees index hit → receipt claims success while stream has no entry. | **EXECUTED:** concurrent in-memory append regression test yields one ledger entry. **PROCESSED, not executed:** run Redis fault-injection tests around the Lua append script; Redis was unavailable. |
| 5 | MEDIUM | **[CONFIRMED; repaired] A cached re-drive receipt was returned for a reused `redrive_id`, without matching the new request.** | [CONFIDENCE: High] The old cache path and new mismatch regression test are direct. | Same ID + different quarantine or business payload → cached success-shaped receipt returned for the wrong request. | **EXECUTED:** same-ID/same-request retry returns its receipt; same-ID/different-payload retry is rejected. |

### Ranking rationale

F1 ranks first because it can authorize an invalid transition while presenting
a success result, though the ordinary schema path lowers its likelihood. F2
affects the normal processor path and can lose output after offset advancement.
F3 and F4 are less frequent crash windows but can suppress retries or create
false ledger receipts; F3 can strand an event for the full TTL. F5 has a
narrower operator/API surface and now fails closed on request mismatch.

### Selected alternatives and overflow

[EVIDENCE] The original architecture also uses a sparse dictionary, not a
vectorized tensor; it does not own a current-state projection; and computed
probability does not gate the demo action. The Rust critique explicitly
questions whether the earlier engine is a tracker, stochastic policy, or
durable audit system. [UNKNOWN] Whether “tracker” requires VITA itself to own
current state or merely record transitions is a product decision, so this is
kept as an unresolved boundary instead of a confirmed defect.

[EVIDENCE] The Python core still has a hard-coded VIP AOV threshold, while the
Rust notes identify that coupling as a design concern. It is not changed
without a product-owned threshold policy. Duplicate edge registration now
fails closed and the registry is immutable; the offline suite covers both.
`AT_RISK` remains a registered state without a transition edge.

[EVIDENCE] The source documents describe unbounded stream scans, seven-day
payload expiry with a longer-lived quarantine index, and an external
downstream deduplication assumption. These remain residual concerns. The
Redis/Kafka cross-system boundary is not exactly-once; output and re-drive
consumers must deduplicate by stable business/re-drive identifiers.

## 3. PACER analysis matrix

Each cell gives the lens status followed by its basis. `SUPPORTED` means the
supplied source or executed test directly supports the statement;
`INFERRED` means a known failure pattern is applied to the source; `UNKNOWN`
means the target environment or product contract is missing.

| Finding / component | P — Procedural | A — Analogous | C — Conceptual | E — Evidence | R — Reference |
|---|---|---|---|---|---|
| F1: Core validation | SUPPORTED: time/AOV checks precede edge evaluation | INFERRED: fail-closed input validation | SUPPORTED: NaN comparison is false | SUPPORTED: source prototype plus executed regression tests | SUPPORTED: supplied Rust critique; `test_direct_engine_calls_quarantine_malformed_values` |
| F2: Kafka output/offset | SUPPORTED: callbacks were not awaited and the loop continued after errors | INFERRED: at-least-once delivery boundary | SUPPORTED: producer acknowledgement and consumer offset are separate | SUPPORTED: original adapter; no broker test | SUPPORTED: `kafka_consumer.py`; runtime falsifier remains unexecuted |
| F3: Idempotency/ledger | SUPPORTED: reservation preceded append | INFERRED: stale idempotency tombstone | SUPPORTED: reservation is not a durable event record | SUPPORTED: original consumer and Redis code | SUPPORTED: `persistence.py`; crash-injection test remains unexecuted |
| F4: Redis ledger | SUPPORTED: claim and append were separate calls | INFERRED: atomic-script dedupe pattern | SUPPORTED: index hit could outlive missing stream record | SUPPORTED: original append sequence; memory regression executed | SUPPORTED: `persistence.py`; Redis execution remains unverified |
| F5: Re-drive request identity | SUPPORTED: cached receipt returned before request comparison | INFERRED: idempotency keys bind request identity | SUPPORTED: ID alone did not identify payload/quarantine | SUPPORTED: source plus executed mismatch test | SUPPORTED: `redrive.py`; regression test executed |
| MCDA-BB v3: bounded score model | SUPPORTED: schema invokes pure normalization/PDF/CDF wrappers | INFERRED: log-space density and continued-fraction incomplete Beta | SUPPORTED: scores are finite and closed-domain bounded | SUPPORTED: tagged reference values plus benchmark tests | SUPPORTED: `mcda_core.py`; `test_mcda_resilience.py` |

## 4. Recursive dependency and synthesis

### Dependency map (depth limit 3)

```text
D0 input event
 └─ D1 schema validation and time derivation
     └─ D2 registered edge + finite temporal result
         └─ D3 durable Redis ledger append (business-hash dedup)
             └─ D4 Kafka outputs and input offset
```

The review stops the primary chain at D3 as requested; D4 is retained as the
immediate boundary because offset ordering is one of the confirmed failures.
No deeper chain is asserted without a running broker or evidence of
downstream-consumer behavior. Re-drive follows a parallel chain:
quarantine payload → integrity/schema/engine checks → Kafka injection →
audit/receipt/done markers. There is no shared transaction between those
systems.

### Identity boundary and minimal repair

[INFERENCE] The smallest stable identity for the current package is a validated
transition event with a registered edge, auditable intent, and durable ledger
receipt—not a vectorized state tracker and not an autonomous action executor.
The implementation therefore keeps rules pure, bounds the probability output,
uses Redis-side atomic append/deduplication, waits for Kafka output delivery
before the adapter returns, stops after handler or offset commit failure to
preserve per-partition offset order, and binds cached re-drive receipts to
request identity. No current-state projection or exactly-once cross-system
transaction is invented.

### Residual risks

- **[UNKNOWN]** The Redis Lua script has not run against Redis in this
  environment. Redis server write failures and recovery behavior still require
  integration/fault-injection testing.
- **[INFERENCE]** Kafka output may be duplicated after a successful send and a
  failed offset commit. Consumers must deduplicate by `business-hash`.
- **[UNKNOWN]** Re-drive injection may succeed while audit/receipt persistence
  fails. The API now raises an explicit uncertain-outcome error; it cannot roll
  back Kafka and requires downstream deduplication.
- **[UNKNOWN]** Production authentication, authorization, TLS, broker ACLs,
  retention, PII handling, and recovery objectives are not specified.
- **[EVIDENCE]** Quarantine payloads expire after seven days by default while
  the index is longer-lived; recovery after expiry is not guaranteed.
- **[UNKNOWN]** Product ownership of current state and whether a probability
  should gate an action remain unprovided decisions.

### Validation

| Test / check | Status | Observation / falsifier |
|---|---|---|
| Offline resilience suite | **EXECUTED** | 56 passed, 9 Redis-parameterized cases skipped, 2 integration cases deselected. |
| Python syntax compilation | **EXECUTED** | `compileall` succeeded for `src`, `tests`, and `scripts`. |
| Project metadata parse | **EXECUTED** | `pyproject.toml` parsed successfully. |
| Wheel packaging | **EXECUTED** | Wheel built and included `dist-info/licenses/LICENSE`. |
| MCDA-BB v3 resilience suite | **EXECUTED** | 26 tests cover closed-domain rejection, finite edge behavior, all reference percentile examples, schema output rounding, and repeatable business hashes. |
| Kafka/Redis integration | **PROCESSED, not executed** | Docker and the Python clients are unavailable; no claim of integration success. |
| Kafka delivery-failure/offset test | **PROCESSED, not executed** | Must run with a broker and an injected producer delivery failure. |
| Redis crash/partial-write test | **PROCESSED, not executed** | Must run against Redis with failures around `EVAL`/stream persistence. |

## 5. Open-source implementation artifacts

- [README.md](./README.md) — scope, run instructions, limitations, and AGPL network-interaction notice.
- [LICENSE](./LICENSE) — GNU AGPL version 3 text.
- [COMMERCIAL-LICENSING.md](./COMMERCIAL-LICENSING.md) — non-granting commercial inquiry notice.
- [core.py](./src/vita/core.py) and [schema.py](./src/vita/schema.py) — pure decision core and fail-closed validation.
- [mcda_core.py](./src/vita/mcda_core.py) — pure bounded-Beta score normalization, density, and CDF.
- [persistence.py](./src/vita/persistence.py) — in-memory atomic adapter and Redis Lua ledger append.
- [redrive.py](./src/vita/redrive.py) — quarantine, integrity/schema revalidation, request-bound receipt, and explicit uncertain outcome.
- [kafka_consumer.py](./src/vita/kafka_consumer.py) — delivery-confirmed output before input offset commit.
- [test_vita_resilience.py](./tests/test_vita_resilience.py) — malformed input, concurrent deduplication, quarantine, and re-drive assertions.
- [test_mcda_resilience.py](./tests/test_mcda_resilience.py) — score boundaries, percentile benchmarks, edge stability, and hash determinism.
- [test_integration.py](./tests/test_integration.py) — Kafka/Redis integration tests; not executed here.

All generated Python modules carry `SPDX-License-Identifier: AGPL-3.0-or-later`.
The source set is user-provided; no third-party dependency or external service
was added beyond the dependencies already declared in the architecture notes.

The MCDA-BB v3 integration pins the reference shape constants
`alpha=10.0361`, `beta=2.5705`. It rejects invalid scores with
`InvalidScoreBoundaryError` instead of coercing or clamping inputs. Density is
computed in log space and the regularized incomplete Beta uses a bounded
continued fraction; both return four-decimal values, including finite exact
domain-edge outputs. Schema metadata remains optional when no `intent_score`
is supplied.
