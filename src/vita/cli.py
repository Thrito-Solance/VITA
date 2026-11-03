"""Minimal operator CLI. Redrive wiring is deployment-specific — see ReDrivePipeline."""
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) 2026 thrito-solance
from __future__ import annotations

import argparse
import json
import os
import sys

from vita.redrive import QuarantineStore


def main(argv=None) -> None:
    p = argparse.ArgumentParser(prog="vita")
    p.add_argument("--redis-url",
                   default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("inspect", help="list compressed quarantine index")
    s.add_argument("--flag")
    s.add_argument("--user-id")
    s.add_argument("--since", type=float)
    s.add_argument("--limit", type=int, default=50)

    s = sub.add_parser("payload", help="load full payload for one quarantine_id")
    s.add_argument("quarantine_id")

    args = p.parse_args(argv)

    import redis as redis_lib
    backend = redis_lib.from_url(args.redis_url, decode_responses=True)
    qs = QuarantineStore(backend)

    if args.cmd == "inspect":
        for r in qs.inspect(flag=args.flag, user_id=args.user_id,
                            since=args.since, limit=args.limit):
            print(json.dumps(r.__dict__, default=str))
    elif args.cmd == "payload":
        print(json.dumps(qs.load_payload(args.quarantine_id), indent=2))
    else:
        sys.exit(f"unknown command {args.cmd!r}")


if __name__ == "__main__":
    main()
