#!/usr/bin/env python3
"""Verify the live data sources before running the pipeline.

Runs the real fetchers from fetch.py against each API and reports whether live
rows came back, so a silent fallback to synthetic data cannot be mistaken for a
working integration. Standard library only, no Docker and nothing to install:

    python3 fetchers/check_sources.py

Exit code is 0 when every source with a key returned usable live rows.
"""
import io
import os
import sys
from contextlib import redirect_stderr

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fetch import SOURCES  # noqa: E402

ENV_KEYS = {"openaq": "OPENAQ_KEY", "airnow": "AIRNOW_KEY", "purpleair": "PURPLEAIR_KEY"}


def load_env(path):
    """Read .env into the environment without a dependency on python-dotenv."""
    if not os.path.exists(path):
        return
    with open(path) as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, _, value = line.partition("=")
            os.environ.setdefault(name.strip(), value.strip())


def probe(name, limit):
    """Return (status, detail, sample) for one source."""
    if not os.getenv(ENV_KEYS[name]):
        return "SKIP", f"no {ENV_KEYS[name]} in .env or environment", None

    fetch, to_dict = SOURCES[name]
    notices = io.StringIO()
    try:
        with redirect_stderr(notices):
            readings = [to_dict(raw) for raw in fetch(limit)]
    except Exception as error:
        return "FAIL", f"fetcher raised {type(error).__name__}: {error}", None

    notice = notices.getvalue().strip().replace("\n", " | ")
    live = [r for r in readings if r["source"] == name]
    if not live:
        return "FAIL", notice or "no rows returned (API reachable but empty)", None

    missing_coords = sum(1 for r in live if r["lat"] is None or r["lon"] is None)
    zero_values = sum(1 for r in live if r["value"] == 0)
    detail = f"{len(live)} live rows"
    if missing_coords:
        detail += f", {missing_coords} without coordinates (these break the PostGIS map)"
    if zero_values:
        detail += f", {zero_values} with value 0 (check the field mapping)"
    status = "WARN" if missing_coords or zero_values else "PASS"
    return status, detail, live[0]


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_env(os.path.join(root, ".env"))
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 5

    print(f"Checking {len(SOURCES)} sources, {limit} readings each\n")
    failed = False
    for name in SOURCES:
        status, detail, sample = probe(name, limit)
        print(f"[{status:4}] {name:10} {detail}")
        if sample:
            print(f"         sample: {sample}")
        if status == "FAIL":
            failed = True
    print()

    if failed:
        print("At least one source with a key failed. The pipeline will still run,")
        print("but it falls back to synthetic data, so results are not live.")
    else:
        print("Sources with keys returned live rows. Safe to run ./local.sh demo real")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
