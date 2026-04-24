#!/usr/bin/env python3
"""Find a filter formula that actually returns all vouchers with AlterID>N.

The simple '$AlterID > 0' filter returns only 1 voucher even though the
no-filter scan shows 7400 vouchers with AlterIDs in the ~32K..~49K range.
Theory: $AlterID may be string-formatted with leading space at collection
header level, so numeric comparison fails. Test several candidate formulas.

Usage:
    python scripts/probe_alterid_formulas.py "<Company>"
"""
from __future__ import annotations

import re
import sys

from tally_db_pipeline.cli import _client
from tally_db_pipeline.tally_client import _xml, _xml_attr


CANDIDATES = [
    ("gt0",            "$AlterID &gt; 0"),
    ("number_gt0",     "$$Number:$AlterID &gt; 0"),
    ("asnumber_gt0",   "$$AsNumber:$AlterID &gt; 0"),
    ("value_gt0",      "$$Value:$AlterID &gt; 0"),
    ("not_empty",      "NOT $$IsEmpty:$AlterID"),
    ("gt_neg1",        "$AlterID &gt; -1"),
    ("neq_empty",      '$AlterID != ""'),
    ("masterid_gt0",   "$$Number:$MasterID &gt; 0"),
]


def build_xml(company: str, formula: str | None) -> str:
    fetch = "Date, GUID, AlterID, MasterID"
    if formula is None:
        filter_xml = ""
        system_xml = ""
    else:
        filter_xml = "<FILTER>F</FILTER>"
        system_xml = f'<SYSTEM TYPE="Formulae" NAME="{_xml_attr("F")}">{formula}</SYSTEM>'
    return (
        "<ENVELOPE><HEADER><VERSION>1</VERSION><TALLYREQUEST>EXPORT</TALLYREQUEST>"
        "<TYPE>COLLECTION</TYPE><ID>FP</ID></HEADER><BODY><DESC><STATICVARIABLES>"
        f"<SVCURRENTCOMPANY>{_xml(company)}</SVCURRENTCOMPANY>"
        "<SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>"
        "</STATICVARIABLES><TDL><TDLMESSAGE>"
        '<COLLECTION NAME="FP" ISINITIALIZE="Yes"><TYPE>Voucher</TYPE>'
        f"{filter_xml}<FETCH>{fetch}</FETCH></COLLECTION>"
        f"{system_xml}"
        "</TDLMESSAGE></TDL></DESC></BODY></ENVELOPE>"
    )


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    company = sys.argv[1]
    client = _client()
    with client:
        baseline = client.probe("no_filter", build_xml(company, None))
        baseline_count = len(re.findall(r"<GUID[^>]*>", baseline["response_xml"] or ""))
        print(f"baseline (no filter): count={baseline_count} dur={baseline.get('duration_ms')}ms")

        for label, formula in CANDIDATES:
            r = client.probe(label, build_xml(company, formula))
            body = r.get("response_xml") or ""
            n = len(re.findall(r"<GUID[^>]*>", body))
            ok = r.get("ok")
            err = r.get("line_error")
            print(f"  {label:20s} formula={formula!r:45s} count={n} dur={r.get('duration_ms')}ms ok={ok} err={err}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
