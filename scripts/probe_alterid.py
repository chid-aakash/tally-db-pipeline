#!/usr/bin/env python3
"""Probe whether Tally returns ALTERID / MASTERID on voucher collections.

Safe-by-design: single HTTP request, tiny date window (1 day ending today),
explicit narrow FETCH list, no full voucher bodies.

Usage:
    python scripts/probe_alterid.py "<Company Name>" [YYYY-MM-DD]

If no date is given, probes the last 1 day up to today.
Prints the raw response XML (trimmed to first N chars) and a parsed summary
of ALTERID / MASTERID occurrences.
"""
from __future__ import annotations

import re
import sys
from datetime import date, timedelta

from tally_db_pipeline.cli import _client
from tally_db_pipeline.tally_client import _xml, _format_tally_report_date


def build_probe_xml(company: str, from_date: str, to_date: str) -> str:
    # Minimal narrow fetch. Includes AlterID/MasterID explicitly.
    # Uses TYPE=Voucher with no VoucherType filter — smallest code surface.
    # Date range via SVFROMDATE/SVTODATE only (no FILTER system formula),
    # to keep the shape of the request identical to Tally's own daybook.
    fetch = "Date, VoucherNumber, VoucherTypeName, GUID, AlterID, MasterID"
    return (
        "<ENVELOPE>"
        "<HEADER>"
        "<VERSION>1</VERSION>"
        "<TALLYREQUEST>EXPORT</TALLYREQUEST>"
        "<TYPE>COLLECTION</TYPE>"
        "<ID>AlterIdProbe</ID>"
        "</HEADER>"
        "<BODY><DESC>"
        "<STATICVARIABLES>"
        f"<SVCURRENTCOMPANY>{_xml(company)}</SVCURRENTCOMPANY>"
        "<SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>"
        f'<SVFROMDATE TYPE="Date">{_xml(_format_tally_report_date(from_date))}</SVFROMDATE>'
        f'<SVTODATE TYPE="Date">{_xml(_format_tally_report_date(to_date))}</SVTODATE>'
        "</STATICVARIABLES>"
        "<TDL><TDLMESSAGE>"
        '<COLLECTION NAME="AlterIdProbe" ISINITIALIZE="Yes">'
        "<TYPE>Voucher</TYPE>"
        f"<FETCH>{fetch}</FETCH>"
        "</COLLECTION>"
        "</TDLMESSAGE></TDL>"
        "</DESC></BODY>"
        "</ENVELOPE>"
    )


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2

    company = sys.argv[1]
    if len(sys.argv) >= 3:
        to_d = date.fromisoformat(sys.argv[2])
    else:
        to_d = date.today()
    from_d = to_d - timedelta(days=1)

    from_s = from_d.isoformat()
    to_s = to_d.isoformat()

    xml_req = build_probe_xml(company, from_s, to_s)
    print(f"Probing company={company!r} range={from_s}..{to_s}")
    print("--- request (truncated) ---")
    print(xml_req[:500])
    print("...")

    client = _client()
    with client:
        result = client.probe("alterid_probe", xml_req)

    print("--- response meta ---")
    print(f"  ok: {result.get('ok')}")
    print(f"  duration_ms: {result.get('duration_ms')}")
    print(f"  error_kind: {result.get('error_kind')}")
    print(f"  line_error: {result.get('line_error')}")

    body = result.get("response_xml") or ""
    print(f"  response_len: {len(body)}")

    alterid_hits = re.findall(r"<ALTERID[^>]*>([^<]*)</ALTERID>", body)
    masterid_hits = re.findall(r"<MASTERID[^>]*>([^<]*)</MASTERID>", body)
    voucher_count = len(re.findall(r"<VOUCHER\b", body))
    guid_hits = re.findall(r"<GUID[^>]*>([^<]*)</GUID>", body)

    print("--- parsed summary ---")
    print(f"  <VOUCHER> elements: {voucher_count}")
    print(f"  <GUID> count: {len(guid_hits)}")
    print(f"  <ALTERID> count: {len(alterid_hits)}")
    print(f"  <MASTERID> count: {len(masterid_hits)}")
    if alterid_hits:
        sample = alterid_hits[:5]
        print(f"  ALTERID samples: {sample}")
        try:
            nums = [int(x) for x in alterid_hits if x.strip()]
            if nums:
                print(f"  ALTERID min={min(nums)} max={max(nums)}")
        except ValueError:
            pass
    if masterid_hits:
        print(f"  MASTERID samples: {masterid_hits[:5]}")

    print("--- response head (first 1500 chars) ---")
    print(body[:1500])
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
