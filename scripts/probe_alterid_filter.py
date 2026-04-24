#!/usr/bin/env python3
"""Test whether Tally's FILTER on $AlterID actually works.

Runs three small requests against a narrow date window:
  A) no filter              -> baseline count
  B) filter $AlterID > HUGE -> expect 0 vouchers
  C) filter $AlterID > 0    -> expect == baseline

Narrow date window keeps blast radius tiny even if filter misbehaves.

Usage:
    python scripts/probe_alterid_filter.py "<Company Name>" [YYYY-MM-DD]
"""
from __future__ import annotations

import re
import sys
from datetime import date, timedelta

from tally_db_pipeline.cli import _client
from tally_db_pipeline.tally_client import _xml, _xml_attr, _format_tally_report_date


def build_xml(company: str, from_date: str, to_date: str, alter_id_gt: int | None) -> str:
    fetch = "Date, VoucherNumber, VoucherTypeName, GUID, AlterID, MasterID"
    filter_xml = ""
    system_xml = ""
    if alter_id_gt is not None:
        filter_name = "AlterIdGtFilter"
        formula = f"$AlterID &gt; {alter_id_gt}"
        filter_xml = f"<FILTER>{filter_name}</FILTER>"
        system_xml = f'<SYSTEM TYPE="Formulae" NAME="{_xml_attr(filter_name)}">{formula}</SYSTEM>'
    return (
        "<ENVELOPE>"
        "<HEADER>"
        "<VERSION>1</VERSION>"
        "<TALLYREQUEST>EXPORT</TALLYREQUEST>"
        "<TYPE>COLLECTION</TYPE>"
        "<ID>AlterIdFilterProbe</ID>"
        "</HEADER>"
        "<BODY><DESC>"
        "<STATICVARIABLES>"
        f"<SVCURRENTCOMPANY>{_xml(company)}</SVCURRENTCOMPANY>"
        "<SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>"
        f'<SVFROMDATE TYPE="Date">{_xml(_format_tally_report_date(from_date))}</SVFROMDATE>'
        f'<SVTODATE TYPE="Date">{_xml(_format_tally_report_date(to_date))}</SVTODATE>'
        "</STATICVARIABLES>"
        "<TDL><TDLMESSAGE>"
        '<COLLECTION NAME="AlterIdFilterProbe" ISINITIALIZE="Yes">'
        "<TYPE>Voucher</TYPE>"
        f"{filter_xml}"
        f"<FETCH>{fetch}</FETCH>"
        "</COLLECTION>"
        f"{system_xml}"
        "</TDLMESSAGE></TDL>"
        "</DESC></BODY>"
        "</ENVELOPE>"
    )


def run(client, label: str, xml_req: str) -> dict:
    result = client.probe(label, xml_req)
    body = result.get("response_xml") or ""
    guid_hits = re.findall(r"<GUID[^>]*>([^<]*)</GUID>", body)
    alterids = [int(x) for x in re.findall(r"<ALTERID[^>]*>\s*(\d+)\s*</ALTERID>", body)]
    summary = {
        "label": label,
        "ok": result.get("ok"),
        "duration_ms": result.get("duration_ms"),
        "line_error": result.get("line_error"),
        "response_len": len(body),
        "guid_count": len(guid_hits),
        "alterid_count": len(alterids),
        "alterid_min": min(alterids) if alterids else None,
        "alterid_max": max(alterids) if alterids else None,
    }
    return summary


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    company = sys.argv[1]
    to_d = date.fromisoformat(sys.argv[2]) if len(sys.argv) >= 3 else date.today()
    # Use a slightly wider window so there's likely >0 vouchers to filter on.
    from_d = to_d - timedelta(days=14)
    from_s, to_s = from_d.isoformat(), to_d.isoformat()
    print(f"Window: {from_s} .. {to_s} (company={company!r})\n")

    client = _client()
    with client:
        a = run(client, "A_no_filter", build_xml(company, from_s, to_s, alter_id_gt=None))
        b = run(client, "B_alterid_gt_huge", build_xml(company, from_s, to_s, alter_id_gt=9_999_999_999))
        c = run(client, "C_alterid_gt_zero", build_xml(company, from_s, to_s, alter_id_gt=0))

    for s in (a, b, c):
        print(s)

    print()
    print("Interpretation:")
    baseline = a["guid_count"]
    if a["line_error"] or b["line_error"] or c["line_error"]:
        print("  [!] One or more requests returned a line_error — filter syntax may be wrong.")
    if b["guid_count"] == 0 and c["guid_count"] == baseline and baseline > 0:
        print(f"  [OK] Filter works: baseline={baseline}, huge_gt=0, zero_gt={c['guid_count']}")
    elif baseline == 0:
        print("  [?] No vouchers in window — widen date range and retry.")
    else:
        print(f"  [??] Unexpected: baseline={baseline} huge_gt={b['guid_count']} zero_gt={c['guid_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
