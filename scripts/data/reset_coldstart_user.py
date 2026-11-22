#!/usr/bin/env python3
"""Wipe all interactions for one demo cold-start user via admin API."""
from __future__ import annotations

import argparse
import sys

import httpx


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--user-id", default="", help="e.g. sv_7313; empty = pick most active")
    args = ap.parse_args()

    body = {}
    if args.user_id.strip():
        body["user_id"] = args.user_id.strip()

    with httpx.Client(timeout=60) as client:
        client.get(f"{args.api}/health").raise_for_status()
        r = client.post(f"{args.api}/api/admin/coldstart-user", json=body)
        if r.status_code >= 400:
            print(r.status_code, r.text, file=sys.stderr)
            return 1
        print(r.json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
