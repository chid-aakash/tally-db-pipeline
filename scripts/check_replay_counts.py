#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

from tally_db_pipeline.db import get_session
from tally_db_pipeline.sync import init_db, replay_xml_bundle


EXPECTED = {
    "masters": {"groups": 282, "ledgers": 2943, "company": "Avinash Industries - Chennai Unit - 2025-26"},
    "stock-groups": {"count": 342},
    "stock-items": {"count": 2750},
    "units": {"count": 24},
    "godowns": {"count": 111},
    "cost-centres": {"count": 311},
    "voucher-types": {"count": 118},
    "vouchers": {"saved": 2},
}


def main() -> int:
    bundle_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/Users/aakashchid/workshop/sena/office/_workspace-admin/data-dumps/tally-xml-exports")
    company_name = sys.argv[2] if len(sys.argv) > 2 else "Avinash Industries - Chennai Unit - 2025-26"

    init_db()
    with get_session() as session:
        results = replay_xml_bundle(session, directory=str(bundle_dir), company_name=company_name)

    by_kind = {row["kind"]: row for row in results}
    failures: list[str] = []

    for kind, expected in EXPECTED.items():
        row = by_kind.get(kind)
        if row is None:
            failures.append(f"Missing result for {kind}")
            continue
        if row.get("error"):
            failures.append(f"{kind} returned error: {row['error']}")
            continue
        for key, expected_value in expected.items():
            actual_value = row.get(key)
            if actual_value != expected_value:
                failures.append(f"{kind}.{key}: expected {expected_value!r}, got {actual_value!r}")

    if failures:
        print(json.dumps({"ok": False, "failures": failures, "results": results}, indent=2))
        return 1

    print(json.dumps({"ok": True, "results": results}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
