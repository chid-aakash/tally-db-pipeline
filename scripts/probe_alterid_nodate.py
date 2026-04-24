#!/usr/bin/env python3
"""Test $AlterID filter WITHOUT a date window — the real target use case.

Uses a high threshold so result count stays small even if the filter misbehaves.
First finds a safe threshold by asking for $$LastAltId via a tiny report-style
request isn't trivial in TDL from pure XML — instead we pass threshold on CLI,
defaulting to (known_max - 20) based on prior probes.

Usage:
    python scripts/probe_alterid_nodate.py "<Company>" [threshold_alter_id]

Safety: if threshold is absurdly low (e.g. 0), this could return ALL vouchers.
Default is 49998 (= known 50018 - 20).
"""
from __future__ import annotations

import re
import sys

from tally_db_pipeline.cli import _client
from tally_db_pipeline.tally_client import _xml, _xml_attr


def build_xml(company: str, alter_id_gt: int) -> str:
    fetch = "Date, VoucherNumber, VoucherTypeName, GUID, AlterID, MasterID"
    filter_name = "AlterIdGtFilter"
    formula = f"$AlterID &gt; {alter_id_gt}"
    return (
        "<ENVELOPE>"
        "<HEADER>"
        "<VERSION>1</VERSION>"
        "<TALLYREQUEST>EXPORT</TALLYREQUEST>"
        "<TYPE>COLLECTION</TYPE>"
        "<ID>AlterIdNoDateProbe</ID>"
        "</HEADER>"
        "<BODY><DESC>"
        "<STATICVARIABLES>"
        f"<SVCURRENTCOMPANY>{_xml(company)}</SVCURRENTCOMPANY>"
        "<SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>"
        "</STATICVARIABLES>"
        "<TDL><TDLMESSAGE>"
        '<COLLECTION NAME="AlterIdNoDateProbe" ISINITIALIZE="Yes">'
        "<TYPE>Voucher</TYPE>"
        f"<FILTER>{filter_name}</FILTER>"
        f"<FETCH>{fetch}</FETCH>"
        "</COLLECTION>"
        f'<SYSTEM TYPE="Formulae" NAME="{_xml_attr(filter_name)}">{formula}</SYSTEM>'
        "</TDLMESSAGE></TDL>"
        "</DESC></BODY>"
        "</ENVELOPE>"
    )


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    company = sys.argv[1]
    threshold = int(sys.argv[2]) if len(sys.argv) >= 3 else 49998

    print(f"Company: {company!r}")
    print(f"Threshold: $AlterID > {threshold} (NO date window)")

    client = _client()
    with client:
        result = client.probe("alterid_nodate", build_xml(company, threshold))

    body = result.get("response_xml") or ""
    guid_hits = re.findall(r"<GUID[^>]*>([^<]*)</GUID>", body)
    alterids = [int(x) for x in re.findall(r"<ALTERID[^>]*>\s*(\d+)\s*</ALTERID>", body)]
    dates = re.findall(r"<DATE[^>]*>([^<]*)</DATE>", body)

    print(f"ok={result.get('ok')} duration_ms={result.get('duration_ms')}")
    print(f"line_error={result.get('line_error')}")
    print(f"response_len={len(body)}")
    print(f"guid_count={len(guid_hits)} alterid_count={len(alterids)}")
    if alterids:
        print(f"alterid min={min(alterids)} max={max(alterids)}")
    if dates:
        uniq = sorted(set(d.strip() for d in dates if d.strip()))
        print(f"date range: {uniq[0]} .. {uniq[-1]} ({len(uniq)} distinct)")
    print()
    print("Interpretation:")
    if len(alterids) > 0 and all(a > threshold for a in alterids):
        print(f"  [OK] Got {len(alterids)} voucher(s), all with AlterID > {threshold}. Filter works without date.")
    elif len(alterids) == 0:
        print(f"  [?] Zero results. Threshold may be above max AlterID. Try lower.")
    else:
        bad = [a for a in alterids if a <= threshold]
        print(f"  [!] {len(bad)} rows have AlterID <= threshold. Filter not being respected.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
