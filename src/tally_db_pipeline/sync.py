from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .db import engine
from .models import (
    Base,
    Company,
    CostCentre,
    Godown,
    Group,
    Ledger,
    RawPayload,
    StockGroup,
    StockItem,
    SyncCheckpoint,
    SyncRun,
    Unit,
    Voucher,
    VoucherInventoryEntry,
    VoucherLedgerEntry,
    VoucherUnknownSection,
    VoucherType,
)
from .parsers import (
    parse_collection,
    parse_company_collection,
    parse_list_of_accounts,
    parse_stock_item_balances,
    parse_vouchers,
    resolve_voucher_base_type,
)
from .tally_client import TallyClient


STANDARD_VOUCHER_TYPES = [
    "Sales",
    "Purchase",
    "Receipt",
    "Payment",
    "Journal",
    "Contra",
    "Credit Note",
    "Debit Note",
]

def init_db() -> None:
    Base.metadata.create_all(bind=engine)


def _start_run(session: Session, sync_type: str, company_name: str | None = None) -> SyncRun:
    run = SyncRun(sync_type=sync_type, company_name=company_name, status="running")
    session.add(run)
    session.commit()
    session.refresh(run)
    return run


def _finish_run(session: Session, run: SyncRun, status: str, error_message: str | None = None) -> None:
    run.status = status
    run.error_message = error_message
    run.finished_at = datetime.utcnow()
    session.add(run)
    session.commit()


def _record_payload(session: Session, run: SyncRun, payload: dict, company_name: str | None = None) -> None:
    row = RawPayload(
        sync_run_id=run.id,
        request_type=payload["request_type"],
        company_name=company_name,
        request_xml=payload["request_xml"],
        response_xml=payload["response_xml"],
        response_sha256=payload["response_sha256"],
    )
    session.add(row)
    session.commit()


def _record_file_payload(
    session: Session,
    run: SyncRun,
    request_type: str,
    file_path: str,
    response_xml: str,
    company_name: str | None = None,
) -> None:
    payload = {
        "request_type": request_type,
        "request_xml": f"FILE://{file_path}",
        "response_xml": response_xml,
        "response_sha256": __import__("hashlib").sha256(response_xml.encode("utf-8")).hexdigest(),
    }
    _record_payload(session, run, payload, company_name=company_name)


def _upsert_checkpoint(
    session: Session,
    *,
    entity_type: str,
    company_name: str | None,
    status: str,
    row_count: int = 0,
    marker: str | None = None,
    error_message: str | None = None,
) -> None:
    normalized_company = company_name or ""
    checkpoint = session.scalar(
        select(SyncCheckpoint).where(
            SyncCheckpoint.entity_type == entity_type,
            SyncCheckpoint.company_name == normalized_company,
        )
    )
    if checkpoint is None:
        checkpoint = SyncCheckpoint(entity_type=entity_type, company_name=normalized_company)
        session.add(checkpoint)
    checkpoint.last_sync_status = status
    checkpoint.last_error_message = error_message
    checkpoint.last_row_count = row_count
    checkpoint.last_marker = marker
    checkpoint.updated_at = datetime.utcnow()
    if status == "success":
        checkpoint.last_success_at = datetime.utcnow()
    session.commit()


def _upsert_by_name(session: Session, model, name: str, values: dict):
    row = session.scalar(select(model).where(model.name == name))
    if row is None:
        row = model(name=name, **values)
        session.add(row)
    else:
        for key, value in values.items():
            setattr(row, key, value)
    setattr(row, "last_synced_at", datetime.utcnow())
    session.commit()
    return row


def _parse_iso_date(raw: str) -> datetime:
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%d-%b-%Y", "%d-%B-%Y"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported date format: {raw}. Use YYYY-MM-DD.")


def _format_iso_date(value: datetime) -> str:
    return value.strftime("%Y-%m-%d")


def infer_company_fiscal_year_start(company_name: str) -> str | None:
    match = re.search(r"(20\d{2})\s*-\s*(\d{2,4})$", company_name.strip())
    if not match:
        return None
    start_year = int(match.group(1))
    return f"{start_year}-04-01"


def _next_day(raw: str) -> str:
    return _format_iso_date(_parse_iso_date(raw) + timedelta(days=1))


def _iter_date_windows(start_date: str, end_date: str, chunk_days: int) -> list[tuple[str, str]]:
    if chunk_days <= 0:
        raise ValueError("chunk_days must be positive.")
    start = _parse_iso_date(start_date)
    end = _parse_iso_date(end_date)
    if start > end:
        raise ValueError("start_date must be earlier than or equal to end_date.")

    windows: list[tuple[str, str]] = []
    cursor = start
    delta = timedelta(days=chunk_days - 1)
    one_day = timedelta(days=1)
    while cursor <= end:
        window_end = min(cursor + delta, end)
        windows.append((_format_iso_date(cursor), _format_iso_date(window_end)))
        cursor = window_end + one_day
    return windows


def _window_day_count(start_date: str, end_date: str) -> int:
    start = _parse_iso_date(start_date)
    end = _parse_iso_date(end_date)
    return (end - start).days + 1


def _split_window(start_date: str, end_date: str) -> tuple[tuple[str, str], tuple[str, str]] | None:
    total_days = _window_day_count(start_date, end_date)
    if total_days <= 1:
        return None
    start = _parse_iso_date(start_date)
    midpoint = start + timedelta(days=(total_days // 2) - 1)
    first_end = _format_iso_date(midpoint)
    second_start = _next_day(first_end)
    return ((start_date, first_end), (second_start, end_date))


def _get_checkpoint(session: Session, *, entity_type: str, company_name: str | None) -> SyncCheckpoint | None:
    normalized_company = company_name or ""
    return session.scalar(
        select(SyncCheckpoint).where(
            SyncCheckpoint.entity_type == entity_type,
            SyncCheckpoint.company_name == normalized_company,
        )
    )


def sync_companies(session: Session, client: TallyClient) -> list[dict]:
    run = _start_run(session, "companies")
    try:
        payload = client.execute("companies", client.build_company_collection_xml())
        _record_payload(session, run, payload)
        companies = parse_company_collection(payload["response_xml"])
        for row in companies:
            if not row["name"]:
                continue
            company = session.scalar(select(Company).where(Company.name == row["name"]))
            if company is None:
                company = Company(name=row["name"])
                session.add(company)
            for key, value in row.items():
                setattr(company, key, value)
            company.last_synced_at = datetime.utcnow()
        session.commit()
        _upsert_checkpoint(session, entity_type="companies", company_name=None, status="success", row_count=len([r for r in companies if r["name"]]))
        _finish_run(session, run, "success")
        return companies
    except Exception as exc:
        session.rollback()
        _upsert_checkpoint(session, entity_type="companies", company_name=None, status="failed", error_message=str(exc))
        _finish_run(session, run, "failed", str(exc))
        raise


def discover_tally(client: TallyClient, company_name: str | None = None) -> dict:
    connection = client.probe("companies_probe", client.build_company_collection_xml())
    if not connection.get("ok"):
        return {
            "connected": False,
            "base_url": client.base_url,
            "companies": [],
            "warnings": [connection.get("error", "Connection failed")],
            "tests": {
                "companies": {
                    "ok": False,
                    "count": 0,
                    "error": connection.get("error"),
                    "error_kind": connection.get("error_kind"),
                    "duration_ms": connection.get("duration_ms"),
                }
            },
        }

    companies = parse_company_collection(connection["response_xml"])
    company_names = [row["name"] for row in companies if row["name"]]
    warnings: list[str] = []
    tests: dict[str, dict] = {}

    tests["companies"] = {
        "ok": bool(company_names),
        "count": len(company_names),
        "error": None,
        "error_kind": None,
        "duration_ms": connection.get("duration_ms"),
    }
    if not company_names:
        warnings.append("No companies were discoverable from Tally. The active company may not be open or exposed.")

    if company_name:
        voucher_type_payload = client.probe(
            "voucher_types_probe",
            client.build_collection_xml(
                "VoucherTypes",
                "Voucher Type",
                fields=["Name", "Parent", "NumberingMethod"],
                company=company_name,
            ),
        )
        voucher_type_rows = parse_collection(voucher_type_payload["response_xml"], "Voucher Type") if voucher_type_payload.get("response_xml") else []
        tests["voucher_types"] = {
            "ok": bool(voucher_type_rows),
            "error": voucher_type_payload.get("error"),
            "error_kind": voucher_type_payload.get("error_kind"),
            "count": len(voucher_type_rows),
            "duration_ms": voucher_type_payload.get("duration_ms"),
        }
        if voucher_type_payload.get("error"):
            warnings.append(f"Voucher type probe error for '{company_name}': {voucher_type_payload['error']}")
        elif not voucher_type_rows:
            warnings.append(f"No voucher types returned for '{company_name}'.")

        masters_probe = client.probe(
            "masters_probe",
            client.build_report_xml("List of Accounts", explode=True, company=company_name),
        )
        masters_probe_accounts = parse_list_of_accounts(masters_probe["response_xml"]) if masters_probe.get("response_xml") else {"groups": [], "ledgers": []}
        master_rows = len(masters_probe_accounts["groups"]) + len(masters_probe_accounts["ledgers"])
        tests["masters"] = {
            "ok": master_rows > 0 and masters_probe.get("error") is None,
            "group_count": len(masters_probe_accounts["groups"]),
            "ledger_count": len(masters_probe_accounts["ledgers"]),
            "error": masters_probe.get("error"),
            "error_kind": masters_probe.get("error_kind"),
            "duration_ms": masters_probe.get("duration_ms"),
        }
        if masters_probe.get("error"):
            warnings.append(f"Master-data probe error for '{company_name}': {masters_probe['error']}")
        elif master_rows == 0:
            warnings.append(f"Master-data probe returned zero groups/ledgers for '{company_name}'.")
    else:
        warnings.append("Voucher-type probe skipped because no company name was provided.")

    return {
        "connected": True,
        "base_url": client.base_url,
        "companies": company_names,
        "warnings": warnings,
        "tests": tests,
    }


def sync_masters(session: Session, client: TallyClient, company_name: str | None = None) -> dict:
    run = _start_run(session, "masters", company_name=company_name)
    try:
        accounts_payload = client.execute("list_of_accounts", client.build_report_xml("List of Accounts", explode=True, company=company_name))
        _record_payload(session, run, accounts_payload, company_name=company_name)
        line_error = client.extract_line_error(accounts_payload["response_xml"])
        if line_error:
            raise RuntimeError(line_error)
        accounts = parse_list_of_accounts(accounts_payload["response_xml"])

        resolved_company_name = company_name or accounts.get("company")
        if resolved_company_name:
            company = session.scalar(select(Company).where(Company.name == resolved_company_name))
            if company is None:
                company = Company(name=resolved_company_name)
                session.add(company)
            company.last_synced_at = datetime.utcnow()
            session.commit()

        if not accounts["groups"] and not accounts["ledgers"]:
            raise RuntimeError(
                "No master data returned from Tally. Open the target company in Tally and retry."
            )

        for group in accounts["groups"]:
            _upsert_by_name(session, Group, group["name"], {k: v for k, v in group.items() if k != "name"})

        for ledger in accounts["ledgers"]:
            _upsert_by_name(session, Ledger, ledger["name"], {k: v for k, v in ledger.items() if k != "name"})

        entities: list[tuple[str, str, type, list[str] | None]] = [
            ("stock_groups", "Stock Group", StockGroup, ["Name", "Parent", "GUID"]),
            ("stock_items", "Stock Item", StockItem, ["Name", "Parent", "BaseUnits", "OpeningBalance", "OpeningQuantity", "OpeningRate", "HSNCode", "GSTApplicable", "GUID"]),
            ("units", "Unit", Unit, ["Name", "OriginalName", "IsSimpleUnit"]),
            ("godowns", "Godown", Godown, ["Name", "Parent"]),
            ("cost_centres", "Cost Centre", CostCentre, ["Name", "Parent", "ForPayroll", "IsEmployeeGroup"]),
        ]

        for request_type, object_type, model, fields in entities:
            payload = client.execute(
                request_type,
                client.build_collection_xml(object_type.replace(" ", ""), object_type, fields=fields, company=resolved_company_name),
            )
            _record_payload(session, run, payload, company_name=resolved_company_name)
            rows = parse_collection(payload["response_xml"], object_type)
            for row in rows:
                _upsert_by_name(session, model, row["name"], {k: v for k, v in row.items() if k != "name"})

        balances_payload = client.execute(
            "stock_item_balances",
            client.build_collection_xml(
                "StockItemBalances",
                "Stock Item",
                fields=["Name", "Parent", "ClosingBalance", "ClosingRate", "ClosingValue"],
                company=resolved_company_name,
            ),
        )
        _record_payload(session, run, balances_payload, company_name=resolved_company_name)
        for balance in parse_stock_item_balances(balances_payload["response_xml"]):
            _upsert_by_name(session, StockItem, balance["name"], {k: v for k, v in balance.items() if k != "name"})

        _finish_run(session, run, "success")
        _upsert_checkpoint(
            session,
            entity_type="masters",
            company_name=resolved_company_name,
            status="success",
            row_count=len(accounts["groups"]) + len(accounts["ledgers"]),
        )
        return {
            "company": resolved_company_name,
            "groups": len(accounts["groups"]),
            "ledgers": len(accounts["ledgers"]),
        }
    except Exception as exc:
        session.rollback()
        _upsert_checkpoint(session, entity_type="masters", company_name=company_name, status="failed", error_message=str(exc))
        _finish_run(session, run, "failed", str(exc))
        raise


def sync_voucher_types(session: Session, client: TallyClient, company_name: str | None = None) -> list[dict]:
    run = _start_run(session, "voucher_types", company_name=company_name)
    try:
        payload = client.execute(
            "voucher_types",
            client.build_collection_xml("VoucherTypes", "Voucher Type", fields=["Name", "Parent", "NumberingMethod"], company=company_name),
        )
        _record_payload(session, run, payload, company_name=company_name)
        line_error = client.extract_line_error(payload["response_xml"])
        if line_error:
            raise RuntimeError(line_error)
        rows = parse_collection(payload["response_xml"], "Voucher Type")
        for row in rows:
            _upsert_by_name(session, VoucherType, row["name"], {k: v for k, v in row.items() if k != "name"})
        _upsert_checkpoint(session, entity_type="voucher_types", company_name=company_name, status="success", row_count=len(rows))
        _finish_run(session, run, "success")
        return rows
    except Exception as exc:
        session.rollback()
        _upsert_checkpoint(session, entity_type="voucher_types", company_name=company_name, status="failed", error_message=str(exc))
        _finish_run(session, run, "failed", str(exc))
        raise


def sync_vouchers(
    session: Session,
    client: TallyClient,
    company_name: str,
    voucher_type: str,
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict:
    run_label = f"vouchers:{voucher_type}"
    if from_date or to_date:
        run_label += ":range"
    run = _start_run(session, run_label, company_name=company_name)
    try:
        voucher_type_rows = [
            {"name": row.name, "parent": row.parent, "numbering_method": row.numbering_method}
            for row in session.scalars(select(VoucherType)).all()
        ]
        if from_date or to_date:
            payload = client.execute(
                "vouchers_daybook",
                client.build_daybook_xml(company_name, voucher_type=voucher_type, from_date=from_date, to_date=to_date),
            )
        else:
            payload = client.execute("vouchers", client.build_voucher_collection_xml(company_name, voucher_type))
        _record_payload(session, run, payload, company_name=company_name)
        line_error = client.extract_line_error(payload["response_xml"])
        if line_error:
            raise RuntimeError(line_error)
        vouchers = parse_vouchers(payload["response_xml"])

        saved = 0
        latest_marker = None
        for row in vouchers:
            guid = row.get("guid")
            if not guid:
                continue

            voucher = session.scalar(select(Voucher).where(Voucher.guid == guid))
            if voucher is None:
                voucher = Voucher(guid=guid, company_name=company_name, voucher_type_name=row["voucher_type_name"])
                session.add(voucher)
                session.flush()
            else:
                voucher.inventory_entries.clear()
                voucher.ledger_entries.clear()
                voucher.unknown_sections.clear()

            voucher.company_name = company_name
            voucher.voucher_type_name = row["voucher_type_name"]
            voucher.base_voucher_type = resolve_voucher_base_type(row["voucher_type_name"], voucher_type_rows)
            voucher.voucher_date = row["voucher_date"]
            voucher.voucher_number = row["voucher_number"]
            voucher.party_name = row["party_name"]
            voucher.narration = row["narration"]
            voucher.party_gstin = row["party_gstin"]
            voucher.place_of_supply = row["place_of_supply"]
            voucher.is_cancelled = row["is_cancelled"]
            voucher.is_optional = row["is_optional"]
            voucher.last_synced_at = datetime.utcnow()

            for item in row["inventory_entries"]:
                voucher.inventory_entries.append(
                    VoucherInventoryEntry(
                        item_name=item["item_name"],
                        quantity=item["quantity"],
                        uom=item["uom"],
                        rate=item["rate"],
                        amount=item["amount"],
                        hsn=item["hsn"],
                        ledger_name=item["ledger_name"],
                        ledger_amount=item["ledger_amount"],
                        is_deemed_positive=item["is_deemed_positive"],
                        gst_rates_json=json.dumps(item["gst_rates"], sort_keys=True),
                    )
                )

            for item in row["ledger_entries"]:
                voucher.ledger_entries.append(
                    VoucherLedgerEntry(
                        ledger_name=item["ledger_name"],
                        amount=item["amount"],
                        is_deemed_positive=item["is_deemed_positive"],
                        is_party_ledger=item["is_party_ledger"],
                        tax_rate=item["tax_rate"],
                        bill_allocations_json=json.dumps(item["bill_allocations"], sort_keys=True),
                        bank_allocations_json=json.dumps(item["bank_allocations"], sort_keys=True),
                    )
                )

            for item in row["unknown_sections"]:
                voucher.unknown_sections.append(
                    VoucherUnknownSection(
                        section_tag=item["tag"],
                        section_xml=item["xml"],
                    )
                )

            saved += 1
            voucher_date = row.get("voucher_date")
            if voucher_date and (latest_marker is None or voucher_date > latest_marker):
                latest_marker = voucher_date

        session.commit()
        _upsert_checkpoint(
            session,
            entity_type=f"vouchers:{voucher_type}",
            company_name=company_name,
            status="success",
            row_count=saved,
            marker=latest_marker,
        )
        _finish_run(session, run, "success")
        return {"voucher_type": voucher_type, "saved": saved, "from_date": from_date, "to_date": to_date}
    except Exception as exc:
        session.rollback()
        _upsert_checkpoint(
            session,
            entity_type=f"vouchers:{voucher_type}",
            company_name=company_name,
            status="failed",
            error_message=str(exc),
        )
        _finish_run(session, run, "failed", str(exc))
        raise


def sync_vouchers_in_chunks(
    session: Session,
    client: TallyClient,
    *,
    company_name: str,
    voucher_type: str,
    start_date: str,
    end_date: str,
    chunk_days: int = 31,
    continue_on_error: bool = False,
    adaptive: bool = True,
    min_chunk_days: int = 1,
) -> list[dict]:
    results: list[dict] = []
    for from_date, to_date in _iter_date_windows(start_date, end_date, chunk_days):
        try:
            results.append(
                sync_vouchers(
                    session,
                    client,
                    company_name=company_name,
                    voucher_type=voucher_type,
                    from_date=from_date,
                    to_date=to_date,
                )
            )
        except Exception as exc:
            window_days = _window_day_count(from_date, to_date)
            if adaptive and window_days > min_chunk_days:
                split_windows = _split_window(from_date, to_date)
                if split_windows is not None:
                    left_window, right_window = split_windows
                    results.extend(
                        sync_vouchers_in_chunks(
                            session,
                            client,
                            company_name=company_name,
                            voucher_type=voucher_type,
                            start_date=left_window[0],
                            end_date=left_window[1],
                            chunk_days=_window_day_count(left_window[0], left_window[1]),
                            continue_on_error=continue_on_error,
                            adaptive=adaptive,
                            min_chunk_days=min_chunk_days,
                        )
                    )
                    results.extend(
                        sync_vouchers_in_chunks(
                            session,
                            client,
                            company_name=company_name,
                            voucher_type=voucher_type,
                            start_date=right_window[0],
                            end_date=right_window[1],
                            chunk_days=_window_day_count(right_window[0], right_window[1]),
                            continue_on_error=continue_on_error,
                            adaptive=adaptive,
                            min_chunk_days=min_chunk_days,
                        )
                    )
                    continue
            result = {
                "voucher_type": voucher_type,
                "saved": 0,
                "from_date": from_date,
                "to_date": to_date,
                "error": str(exc),
            }
            results.append(result)
            if not continue_on_error:
                raise
    return results


def sync_vouchers_incremental(
    session: Session,
    client: TallyClient,
    *,
    company_name: str,
    voucher_type: str,
    since_date: str | None = None,
    until_date: str | None = None,
    chunk_days: int = 31,
    continue_on_error: bool = False,
    adaptive: bool = True,
    min_chunk_days: int = 1,
) -> list[dict]:
    checkpoint = _get_checkpoint(session, entity_type=f"vouchers:{voucher_type}", company_name=company_name)
    start_date = since_date
    if not start_date and checkpoint and checkpoint.last_marker:
        start_date = _next_day(checkpoint.last_marker)
    if not start_date:
        start_date = infer_company_fiscal_year_start(company_name)
    if not start_date:
        raise ValueError(
            f"No checkpoint exists yet for {voucher_type}, and no fiscal-year suffix could be inferred from the company name. Provide since_date for the initial incremental sync."
        )
    end_date = until_date or _format_iso_date(datetime.utcnow())
    return sync_vouchers_in_chunks(
        session,
        client,
        company_name=company_name,
        voucher_type=voucher_type,
        start_date=start_date,
        end_date=end_date,
        chunk_days=chunk_days,
        continue_on_error=continue_on_error,
        adaptive=adaptive,
        min_chunk_days=min_chunk_days,
    )


def profile_vouchers(
    session: Session,
    client: TallyClient,
    *,
    company_name: str,
    from_date: str,
    to_date: str,
) -> dict:
    run = _start_run(session, "voucher_profile", company_name=company_name)
    try:
        payload = client.execute(
            "voucher_profile_daybook",
            client.build_daybook_xml(company_name, from_date=from_date, to_date=to_date),
        )
        _record_payload(session, run, payload, company_name=company_name)
        line_error = client.extract_line_error(payload["response_xml"])
        if line_error:
            raise RuntimeError(line_error)

        voucher_type_rows = [
            {"name": row.name, "parent": row.parent, "numbering_method": row.numbering_method}
            for row in session.scalars(select(VoucherType)).all()
        ]
        vouchers = parse_vouchers(payload["response_xml"])
        by_type: dict[str, dict] = {}
        for row in vouchers:
            name = row["voucher_type_name"] or "unknown"
            stats = by_type.setdefault(
                name,
                {
                    "voucher_type_name": name,
                    "base_voucher_type": resolve_voucher_base_type(name, voucher_type_rows),
                    "count": 0,
                    "first_date": None,
                    "last_date": None,
                },
            )
            stats["count"] += 1
            voucher_date = row.get("voucher_date")
            if voucher_date:
                if stats["first_date"] is None or voucher_date < stats["first_date"]:
                    stats["first_date"] = voucher_date
                if stats["last_date"] is None or voucher_date > stats["last_date"]:
                    stats["last_date"] = voucher_date

        results = sorted(by_type.values(), key=lambda row: (-row["count"], row["voucher_type_name"]))
        _finish_run(session, run, "success")
        _upsert_checkpoint(
            session,
            entity_type="voucher_profile",
            company_name=company_name,
            status="success",
            row_count=len(vouchers),
            marker=to_date,
        )
        return {
            "company_name": company_name,
            "from_date": from_date,
            "to_date": to_date,
            "total_vouchers": len(vouchers),
            "voucher_types": results,
        }
    except Exception as exc:
        session.rollback()
        _upsert_checkpoint(
            session,
            entity_type="voucher_profile",
            company_name=company_name,
            status="failed",
            error_message=str(exc),
        )
        _finish_run(session, run, "failed", str(exc))
        raise


def profile_vouchers_in_chunks(
    session: Session,
    client: TallyClient,
    *,
    company_name: str,
    start_date: str,
    end_date: str,
    chunk_days: int = 31,
    adaptive: bool = True,
    min_chunk_days: int = 1,
    continue_on_error: bool = False,
) -> dict:
    aggregate: dict[str, dict] = {}
    windows: list[dict] = []

    for from_date, to_date in _iter_date_windows(start_date, end_date, chunk_days):
        try:
            result = profile_vouchers(
                session,
                client,
                company_name=company_name,
                from_date=from_date,
                to_date=to_date,
            )
            windows.append(
                {
                    "from_date": from_date,
                    "to_date": to_date,
                    "total_vouchers": result["total_vouchers"],
                    "voucher_type_count": len(result["voucher_types"]),
                }
            )
            for item in result["voucher_types"]:
                row = aggregate.setdefault(
                    item["voucher_type_name"],
                    {
                        "voucher_type_name": item["voucher_type_name"],
                        "base_voucher_type": item["base_voucher_type"],
                        "count": 0,
                        "first_date": None,
                        "last_date": None,
                    },
                )
                row["count"] += item["count"]
                if item["first_date"] and (row["first_date"] is None or item["first_date"] < row["first_date"]):
                    row["first_date"] = item["first_date"]
                if item["last_date"] and (row["last_date"] is None or item["last_date"] > row["last_date"]):
                    row["last_date"] = item["last_date"]
        except Exception as exc:
            window_days = _window_day_count(from_date, to_date)
            if adaptive and window_days > min_chunk_days:
                split_windows = _split_window(from_date, to_date)
                if split_windows is not None:
                    left_window, right_window = split_windows
                    left_result = profile_vouchers_in_chunks(
                        session,
                        client,
                        company_name=company_name,
                        start_date=left_window[0],
                        end_date=left_window[1],
                        chunk_days=_window_day_count(left_window[0], left_window[1]),
                        adaptive=adaptive,
                        min_chunk_days=min_chunk_days,
                        continue_on_error=continue_on_error,
                    )
                    right_result = profile_vouchers_in_chunks(
                        session,
                        client,
                        company_name=company_name,
                        start_date=right_window[0],
                        end_date=right_window[1],
                        chunk_days=_window_day_count(right_window[0], right_window[1]),
                        adaptive=adaptive,
                        min_chunk_days=min_chunk_days,
                        continue_on_error=continue_on_error,
                    )
                    windows.extend(left_result["windows"])
                    windows.extend(right_result["windows"])
                    for partial in (left_result["voucher_types"], right_result["voucher_types"]):
                        for item in partial:
                            row = aggregate.setdefault(
                                item["voucher_type_name"],
                                {
                                    "voucher_type_name": item["voucher_type_name"],
                                    "base_voucher_type": item["base_voucher_type"],
                                    "count": 0,
                                    "first_date": None,
                                    "last_date": None,
                                },
                            )
                            row["count"] += item["count"]
                            if item["first_date"] and (row["first_date"] is None or item["first_date"] < row["first_date"]):
                                row["first_date"] = item["first_date"]
                            if item["last_date"] and (row["last_date"] is None or item["last_date"] > row["last_date"]):
                                row["last_date"] = item["last_date"]
                    continue

            error_window = {
                "from_date": from_date,
                "to_date": to_date,
                "error": str(exc),
            }
            windows.append(error_window)
            if not continue_on_error:
                raise

    voucher_types = sorted(aggregate.values(), key=lambda row: (-row["count"], row["voucher_type_name"]))
    return {
        "company_name": company_name,
        "from_date": start_date,
        "to_date": end_date,
        "total_vouchers": sum(item["count"] for item in voucher_types),
        "voucher_types": voucher_types,
        "windows": windows,
    }


def sync_standard_vouchers(
    session: Session,
    client: TallyClient,
    company_name: str,
    continue_on_error: bool = False,
) -> list[dict]:
    results: list[dict] = []
    for voucher_type in STANDARD_VOUCHER_TYPES:
        try:
            results.append(sync_vouchers(session, client, company_name=company_name, voucher_type=voucher_type))
        except Exception as exc:
            results.append({"voucher_type": voucher_type, "saved": 0, "error": str(exc)})
            if not continue_on_error:
                raise
    return results


def replay_xml_file(
    session: Session,
    *,
    kind: str,
    file_path: str,
    company_name: str | None = None,
) -> dict:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(file_path)

    xml_text = path.read_text(encoding="utf-8", errors="replace")
    run = _start_run(session, f"replay:{kind}", company_name=company_name)
    try:
        _record_file_payload(session, run, f"replay:{kind}", str(path), xml_text, company_name=company_name)

        if kind == "masters":
            parsed = parse_list_of_accounts(xml_text)
            resolved_company = company_name or parsed.get("company")
            if not parsed["groups"] and not parsed["ledgers"]:
                raise RuntimeError("No groups or ledgers found in XML file.")

            if resolved_company:
                company = session.scalar(select(Company).where(Company.name == resolved_company))
                if company is None:
                    company = Company(name=resolved_company)
                    session.add(company)
                company.last_synced_at = datetime.utcnow()
                session.commit()

            for group in parsed["groups"]:
                _upsert_by_name(session, Group, group["name"], {k: v for k, v in group.items() if k != "name"})
            for ledger in parsed["ledgers"]:
                _upsert_by_name(session, Ledger, ledger["name"], {k: v for k, v in ledger.items() if k != "name"})

            _upsert_checkpoint(
                session,
                entity_type="replay:masters",
                company_name=resolved_company,
                status="success",
                row_count=len(parsed["groups"]) + len(parsed["ledgers"]),
            )
            _finish_run(session, run, "success")
            return {
                "kind": kind,
                "company": resolved_company,
                "groups": len(parsed["groups"]),
                "ledgers": len(parsed["ledgers"]),
            }

        if kind in {"stock-groups", "stock-items", "units", "godowns", "cost-centres", "voucher-types"}:
            object_type_map = {
                "stock-groups": ("Stock Group", StockGroup),
                "stock-items": ("Stock Item", StockItem),
                "units": ("Unit", Unit),
                "godowns": ("Godown", Godown),
                "cost-centres": ("Cost Centre", CostCentre),
                "voucher-types": ("Voucher Type", VoucherType),
            }
            object_type, model = object_type_map[kind]
            rows = parse_collection(xml_text, object_type)
            for row in rows:
                _upsert_by_name(session, model, row["name"], {k: v for k, v in row.items() if k != "name"})
            _upsert_checkpoint(session, entity_type=f"replay:{kind}", company_name=company_name, status="success", row_count=len(rows))
            _finish_run(session, run, "success")
            return {"kind": kind, "count": len(rows)}

        if kind == "stock-item-balances":
            rows = parse_stock_item_balances(xml_text)
            for row in rows:
                _upsert_by_name(session, StockItem, row["name"], {k: v for k, v in row.items() if k != "name"})
            _upsert_checkpoint(session, entity_type=f"replay:{kind}", company_name=company_name, status="success", row_count=len(rows))
            _finish_run(session, run, "success")
            return {"kind": kind, "count": len(rows)}

        if kind == "vouchers":
            if not company_name:
                raise RuntimeError("company_name is required for voucher replay.")
            voucher_type_rows = [
                {"name": row.name, "parent": row.parent, "numbering_method": row.numbering_method}
                for row in session.scalars(select(VoucherType)).all()
            ]
            vouchers = parse_vouchers(xml_text)
            saved = 0
            latest_marker = None
            for row in vouchers:
                guid = row.get("guid")
                if not guid:
                    continue
                voucher = session.scalar(select(Voucher).where(Voucher.guid == guid))
                if voucher is None:
                    voucher = Voucher(guid=guid, company_name=company_name, voucher_type_name=row["voucher_type_name"])
                    session.add(voucher)
                    session.flush()
                else:
                    voucher.inventory_entries.clear()
                    voucher.ledger_entries.clear()
                    voucher.unknown_sections.clear()

                voucher.company_name = company_name
                voucher.voucher_type_name = row["voucher_type_name"]
                voucher.base_voucher_type = resolve_voucher_base_type(row["voucher_type_name"], voucher_type_rows)
                voucher.voucher_date = row["voucher_date"]
                voucher.voucher_number = row["voucher_number"]
                voucher.party_name = row["party_name"]
                voucher.narration = row["narration"]
                voucher.party_gstin = row["party_gstin"]
                voucher.place_of_supply = row["place_of_supply"]
                voucher.is_cancelled = row["is_cancelled"]
                voucher.is_optional = row["is_optional"]
                voucher.last_synced_at = datetime.utcnow()

                for item in row["inventory_entries"]:
                    voucher.inventory_entries.append(
                        VoucherInventoryEntry(
                            item_name=item["item_name"],
                            quantity=item["quantity"],
                            uom=item["uom"],
                            rate=item["rate"],
                            amount=item["amount"],
                            hsn=item["hsn"],
                            ledger_name=item["ledger_name"],
                            ledger_amount=item["ledger_amount"],
                            is_deemed_positive=item["is_deemed_positive"],
                            gst_rates_json=json.dumps(item["gst_rates"], sort_keys=True),
                        )
                    )
                for item in row["ledger_entries"]:
                    voucher.ledger_entries.append(
                        VoucherLedgerEntry(
                            ledger_name=item["ledger_name"],
                            amount=item["amount"],
                            is_deemed_positive=item["is_deemed_positive"],
                            is_party_ledger=item["is_party_ledger"],
                            tax_rate=item["tax_rate"],
                            bill_allocations_json=json.dumps(item["bill_allocations"], sort_keys=True),
                            bank_allocations_json=json.dumps(item["bank_allocations"], sort_keys=True),
                        )
                    )
                for item in row["unknown_sections"]:
                    voucher.unknown_sections.append(
                        VoucherUnknownSection(
                            section_tag=item["tag"],
                            section_xml=item["xml"],
                        )
                    )
                saved += 1
                voucher_date = row.get("voucher_date")
                if voucher_date and (latest_marker is None or voucher_date > latest_marker):
                    latest_marker = voucher_date
            session.commit()
            _upsert_checkpoint(
                session,
                entity_type=f"replay:{kind}",
                company_name=company_name,
                status="success",
                row_count=saved,
                marker=latest_marker,
            )
            _finish_run(session, run, "success")
            return {"kind": kind, "saved": saved}

        raise ValueError(f"Unsupported replay kind: {kind}")
    except Exception as exc:
        session.rollback()
        _upsert_checkpoint(session, entity_type=f"replay:{kind}", company_name=company_name, status="failed", error_message=str(exc))
        _finish_run(session, run, "failed", str(exc))
        raise


def replay_xml_bundle(
    session: Session,
    *,
    directory: str,
    company_name: str,
) -> list[dict]:
    bundle_dir = Path(directory)
    if not bundle_dir.is_dir():
        raise NotADirectoryError(directory)

    plan = [
        ("masters", bundle_dir / "list-of-accounts.xml"),
        ("stock-groups", bundle_dir / "stock-groups.xml"),
        ("stock-items", bundle_dir / "stock-items.xml"),
        ("units", bundle_dir / "units.xml"),
        ("godowns", bundle_dir / "godowns.xml"),
        ("cost-centres", bundle_dir / "cost-centres.xml"),
        ("voucher-types", bundle_dir / "voucher-types.xml"),
        ("vouchers", bundle_dir / "day-book.xml"),
    ]

    results: list[dict] = []
    for kind, file_path in plan:
        if not file_path.exists():
            results.append({"kind": kind, "error": f"Missing file: {file_path.name}"})
            continue
        kwargs = {"kind": kind, "file_path": str(file_path)}
        if kind == "vouchers":
            kwargs["company_name"] = company_name
        results.append(replay_xml_file(session, **kwargs))
    return results


def get_database_report(session: Session) -> dict:
    def count(model) -> int:
        return session.scalar(select(func.count()).select_from(model)) or 0

    recent_runs = [
        {
            "id": row.id,
            "sync_type": row.sync_type,
            "company_name": row.company_name,
            "status": row.status,
            "started_at": row.started_at.isoformat(timespec="seconds") if row.started_at else None,
            "finished_at": row.finished_at.isoformat(timespec="seconds") if row.finished_at else None,
            "error_message": row.error_message,
        }
        for row in session.scalars(select(SyncRun).order_by(SyncRun.id.desc()).limit(10)).all()
    ]

    latest_voucher_syncs: dict[str, str] = {}
    for row in recent_runs:
        if row["sync_type"].startswith("vouchers:"):
            latest_voucher_syncs[row["sync_type"].split(":", 1)[1]] = row["status"]

    return {
        "companies": count(Company),
        "groups": count(Group),
        "ledgers": count(Ledger),
        "stock_groups": count(StockGroup),
        "stock_items": count(StockItem),
        "units": count(Unit),
        "godowns": count(Godown),
        "cost_centres": count(CostCentre),
        "voucher_types": count(VoucherType),
        "vouchers": count(Voucher),
        "voucher_inventory_entries": count(VoucherInventoryEntry),
        "voucher_ledger_entries": count(VoucherLedgerEntry),
        "voucher_unknown_sections": count(VoucherUnknownSection),
        "raw_payloads": count(RawPayload),
        "checkpoints": [
            {
                "entity_type": row.entity_type,
                "company_name": row.company_name or None,
                "last_success_at": row.last_success_at.isoformat(timespec="seconds") if row.last_success_at else None,
                "last_sync_status": row.last_sync_status,
                "last_error_message": row.last_error_message,
                "last_row_count": row.last_row_count,
                "last_marker": row.last_marker,
            }
            for row in session.scalars(select(SyncCheckpoint).order_by(SyncCheckpoint.entity_type, SyncCheckpoint.company_name)).all()
        ],
        "running_syncs": count_running_syncs(session),
        "latest_voucher_syncs": latest_voucher_syncs,
        "recent_runs": recent_runs,
    }


def count_running_syncs(session: Session) -> int:
    return session.scalar(select(func.count()).select_from(SyncRun).where(SyncRun.status == "running")) or 0
