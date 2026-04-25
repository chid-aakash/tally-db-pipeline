from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.orm import Session


DEFAULT_EXCLUDE_PATTERNS: tuple[str, ...] = (
    "00A %",        # petty cash / small-bank ledgers shared across types
    "00L %",        # OLCC bank ledgers shared across types
    "90A %",        # GST input ledgers
    "90L %",        # GST output ledgers
    "10E Round Off%",
)


def find_ledger_prefix_mismatches(
    session: Session,
    *,
    company_name: str | None = None,
    exclude_ledger_like: Iterable[str] = DEFAULT_EXCLUDE_PATTERNS,
) -> list[dict]:
    # Flags voucher_ledger_entries where the first character of the ledger name
    # and of the voucher type are both digits but disagree — e.g. ledger
    # "20E Rework Conveyances" (prefix 2) inside voucher type "12 Payment"
    # (prefix 1). Returns one row per offending ledger entry.
    excludes = list(exclude_ledger_like)
    exclude_sql = " ".join(
        f"AND vle.ledger_name NOT LIKE :excl_{i}" for i, _ in enumerate(excludes)
    )
    company_sql = "AND v.company_name = :company" if company_name else ""
    sql = text(
        f"""
        SELECT v.company_name AS company_name,
               v.voucher_date AS voucher_date,
               v.voucher_type_name AS voucher_type,
               substr(v.voucher_type_name, 1, 1) AS vtype_first,
               v.voucher_number AS voucher_number,
               vle.ledger_name AS ledger_name,
               substr(vle.ledger_name, 1, 1) AS ledger_first,
               vle.amount AS amount,
               vle.is_party_ledger AS is_party_ledger,
               v.party_name AS party_name
        FROM voucher_ledger_entries vle
        JOIN vouchers v ON v.id = vle.voucher_id
        WHERE substr(v.voucher_type_name, 1, 1) BETWEEN '0' AND '9'
          AND substr(vle.ledger_name, 1, 1) BETWEEN '0' AND '9'
          AND substr(v.voucher_type_name, 1, 1) <> substr(vle.ledger_name, 1, 1)
          {exclude_sql}
          {company_sql}
        ORDER BY v.company_name, v.voucher_date, v.voucher_number, vle.ledger_name
        """
    )
    params: dict = {f"excl_{i}": pat for i, pat in enumerate(excludes)}
    if company_name:
        params["company"] = company_name
    return [dict(row._mapping) for row in session.execute(sql, params)]


def export_ledger_prefix_mismatches(
    session: Session,
    output_path: str | Path,
    *,
    company_name: str | None = None,
    exclude_ledger_like: Iterable[str] = DEFAULT_EXCLUDE_PATTERNS,
) -> dict:
    rows = find_ledger_prefix_mismatches(
        session,
        company_name=company_name,
        exclude_ledger_like=exclude_ledger_like,
    )
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "company_name",
        "voucher_date",
        "voucher_type",
        "vtype_first",
        "voucher_number",
        "ledger_name",
        "ledger_first",
        "amount",
        "is_party_ledger",
        "party_name",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return {"row_count": len(rows), "output_path": str(path)}
