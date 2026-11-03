"""Seed demo payloads: healthy + four poison classes."""
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) 2026 thrito-solance
from __future__ import annotations

import json
import os
import time

from confluent_kafka import Producer


def main() -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.environ.get("VITA_INPUT_TOPIC", "vita.raw.events")
    p = Producer({"bootstrap.servers": bootstrap})
    now = time.time()

    cases = [
        # healthy
        {"user_id": "SEED_OK", "current_state": "ACTIVE", "target_state": "DORMANT",
         "event_time": now, "state_entry_time": now - 60, "average_order_value": 50.0},
        # NaN AOV → PX_06
        {"user_id": "SEED_NAN", "current_state": "ACTIVE", "target_state": "DORMANT",
         "event_time": now, "state_entry_time": now - 60,
         "average_order_value": float("nan")},
        # missing edge → PX_02
        {"user_id": "SEED_EDGE", "current_state": "ACTIVE", "target_state": "EMERGING_VIP",
         "event_time": now, "state_entry_time": now - 60, "average_order_value": 500.0},
        # ghost clock → PX_03
        {"user_id": "SEED_GHOST", "current_state": "ACTIVE", "target_state": "DORMANT",
         "event_time": now, "state_entry_time": now + 60, "average_order_value": 50.0},
        # missing field → PX_04
        {"user_id": "SEED_MISSING", "current_state": "ACTIVE", "target_state": "DORMANT",
         "event_time": now, "average_order_value": 50.0},
    ]
    for c in cases:
        p.produce(topic, key=c["user_id"], value=json.dumps(c, allow_nan=True))
    p.flush(timeout=10)
    print(f"seeded {len(cases)} records into {topic}")


if __name__ == "__main__":
    main()
