from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from .config import get_settings
from .db import engine, ensure_runtime_schema
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

STANDARD_BASE_VOUCHER_TYPES = set(STANDARD_VOUCHER_TYPES) | {
    "Sales Order",
    "Purchase Order",
    "Delivery Note",
    "Receipt Note",
    "Stock Journal",
    "Physical Stock",
    "Memorandum",
    "Rejections In",
    "Rejections Out",
    "Payroll",
}


class VoucherRangeValidationError(RuntimeError):
    pass

_COMPANY_FY_SUFFIX_RE = re.compile(r"^(?P<stem>.*?)(?:\s*-\s*(?P<start>20\d{2})\s*-\s*(?P<end>\d{2,4}))$")

def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    ensure_runtime_schema()


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


def _upsert_by_name(session: Session, model, name: str, values: dict, *, company_name: str | None = None):
    scoped_company = company_name or ""
    query = select(model).where(model.name == name)
    if hasattr(model, "company_name"):
        query = query.where(model.company_name == scoped_company)
        values = {"company_name": scoped_company, **values}
    row = session.scalar(query)
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
    parsed = parse_company_name_metadata(company_name)
    if not parsed or parsed.get("start_year") is None:
        return None
    start_year = int(parsed["start_year"])
    return f"{start_year}-04-01"


def infer_company_fiscal_year_end(company_name: str) -> str | None:
    parsed = parse_company_name_metadata(company_name)
    if not parsed or parsed.get("start_year") is None:
        return None
    start_year = int(parsed["start_year"])
    return f"{start_year + 1}-03-31"


def parse_company_name_metadata(company_name: str) -> dict:
    raw_name = " ".join((company_name or "").strip().split())
    match = _COMPANY_FY_SUFFIX_RE.match(raw_name)
    if not match:
        return {
            "name": raw_name,
            "stem": raw_name,
            "normalized_stem": _normalize_company_stem(raw_name),
            "has_fiscal_suffix": False,
            "start_year": None,
            "end_year": None,
            "inferred_start_date": None,
            "inferred_end_date": None,
        }

    start_year = int(match.group("start"))
    end_year_raw = match.group("end")
    end_year = int(end_year_raw)
    if end_year < 100:
        end_year += (start_year // 100) * 100
    stem = " ".join(match.group("stem").split())
    return {
        "name": raw_name,
        "stem": stem,
        "normalized_stem": _normalize_company_stem(stem),
        "has_fiscal_suffix": True,
        "start_year": start_year,
        "end_year": end_year,
        "inferred_start_date": f"{start_year}-04-01",
        "inferred_end_date": f"{start_year + 1}-03-31",
    }


def _normalize_company_stem(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip()).casefold()


def summarize_company_families(company_names: list[str]) -> list[dict]:
    grouped: dict[str, dict] = {}
    for name in company_names:
        metadata = parse_company_name_metadata(name)
        family = grouped.setdefault(
            metadata["normalized_stem"],
            {
                "stem": metadata["stem"],
                "normalized_stem": metadata["normalized_stem"],
                "company_count": 0,
                "companies": [],
            },
        )
        family["companies"].append(metadata)

    families = list(grouped.values())
    for family in families:
        family["companies"].sort(
            key=lambda row: (
                row["start_year"] is None,
                row["start_year"] or 0,
                row["name"],
            )
        )
        family["company_count"] = len(family["companies"])
        family["fiscal_years"] = [
            f"{row['start_year']}-{str(row['end_year'])[-2:]}"
            for row in family["companies"]
            if row["start_year"] is not None and row["end_year"] is not None
        ]
    families.sort(key=lambda row: (-row["company_count"], row["stem"]))
    return families


def resolve_company_family(company_names: list[str], selector: str) -> list[dict]:
    selector_meta = parse_company_name_metadata(selector)
    selector_name = selector_meta["name"]
    selector_stem = selector_meta["normalized_stem"]
    exact_matches = [parse_company_name_metadata(name) for name in company_names if name == selector_name]
    if exact_matches and exact_matches[0]["has_fiscal_suffix"]:
        return [
            row
            for row in (parse_company_name_metadata(name) for name in company_names)
            if row["normalized_stem"] == selector_stem
        ]
    if exact_matches:
        return exact_matches

    family_matches = [
        row
        for row in (parse_company_name_metadata(name) for name in company_names)
        if row["normalized_stem"] == selector_stem
    ]
    if family_matches:
        return family_matches
    raise ValueError(f"No visible Tally companies matched selector: {selector}")


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


def _validate_voucher_dates_within_range(vouchers: list[dict], from_date: str | None, to_date: str | None) -> None:
    if not vouchers or (from_date is None and to_date is None):
        return

    start = _parse_iso_date(from_date) if from_date else None
    end = _parse_iso_date(to_date) if to_date else None
    out_of_range: list[str] = []
    missing_dates = 0
    for row in vouchers:
        raw_date = row.get("voucher_date")
        if not raw_date:
            missing_dates += 1
            continue
        parsed = _parse_iso_date(raw_date)
        if start and parsed < start:
            out_of_range.append(raw_date)
            continue
        if end and parsed > end:
            out_of_range.append(raw_date)

    if missing_dates or out_of_range:
        details: list[str] = []
        if out_of_range:
            unique_dates = sorted(set(out_of_range))
            sample = ", ".join(unique_dates[:5])
            if len(unique_dates) > 5:
                sample += ", ..."
            details.append(f"out-of-range dates: {sample}")
        if missing_dates:
            details.append(f"missing dates: {missing_dates}")
        requested = f"{from_date or '?'}..{to_date or '?'}"
        raise VoucherRangeValidationError(
            "Tally returned voucher rows outside the requested date window "
            f"({requested}); refusing to treat this as a valid chunked/incremental response ({'; '.join(details)})."
        )


def _get_checkpoint(session: Session, *, entity_type: str, company_name: str | None) -> SyncCheckpoint | None:
    normalized_company = company_name or ""
    return session.scalar(
        select(SyncCheckpoint).where(
            SyncCheckpoint.entity_type == entity_type,
            SyncCheckpoint.company_name == normalized_company,
        )
    )


def _load_voucher_type_rows(session: Session, company_name: str | None = None) -> list[dict]:
    scoped_company = company_name or ""
    rows = session.scalars(
        select(VoucherType).where(
            (VoucherType.company_name == scoped_company) | (VoucherType.company_name == "")
        )
    ).all()
    return [
        {"name": row.name, "parent": row.parent, "numbering_method": row.numbering_method, "company_name": row.company_name}
        for row in rows
    ]


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
        error_kind = connection.get("error_kind")
        recommended_actions = _recommended_actions_for_error_kind(error_kind)
        return {
            "connected": False,
            "base_url": client.base_url,
            "companies": [],
            "warnings": [connection.get("error", "Connection failed")],
            "health_status": _health_status_for_error_kind(error_kind),
            "recommended_actions": recommended_actions,
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
    company_families = summarize_company_families(company_names)
    warnings: list[str] = []
    recommended_actions: list[str] = []
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
        recommended_actions.extend(
            [
                "Make sure at least one company is open in the Tally UI.",
                "Re-run `tally-db-pipeline list-companies` after opening the target company.",
            ]
        )

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
            recommended_actions.extend(_recommended_actions_for_error_kind(voucher_type_payload.get("error_kind")))
        elif not voucher_type_rows:
            warnings.append(f"No voucher types returned for '{company_name}'.")
            recommended_actions.append("Run `tally-db-pipeline sync-voucher-types --company \"Exact Company Name\"` only after verifying the company is fully loaded in Tally.")

        groups_probe = client.probe(
            "groups_probe",
            client.build_collection_xml(
                "GroupsProbe",
                "Group",
                fields=["Name", "Parent", "GUID"],
                company=company_name,
            ),
        )
        groups_rows = parse_collection(groups_probe["response_xml"], "Group") if groups_probe.get("response_xml") else []
        ledgers_probe = client.probe(
            "ledgers_probe",
            client.build_collection_xml(
                "LedgersProbe",
                "Ledger",
                fields=["Name", "Parent", "GUID"],
                company=company_name,
            ),
        )
        ledgers_rows = parse_collection(ledgers_probe["response_xml"], "Ledger") if ledgers_probe.get("response_xml") else []
        master_rows = len(groups_rows) + len(ledgers_rows)
        tests["masters"] = {
            "ok": master_rows > 0 and groups_probe.get("error") is None and ledgers_probe.get("error") is None,
            "group_count": len(groups_rows),
            "ledger_count": len(ledgers_rows),
            "error": groups_probe.get("error") or ledgers_probe.get("error"),
            "error_kind": groups_probe.get("error_kind") or ledgers_probe.get("error_kind"),
            "duration_ms": (groups_probe.get("duration_ms") or 0) + (ledgers_probe.get("duration_ms") or 0),
        }
        if groups_probe.get("error") or ledgers_probe.get("error"):
            error_message = groups_probe.get("error") or ledgers_probe.get("error")
            error_kind = groups_probe.get("error_kind") or ledgers_probe.get("error_kind")
            warnings.append(f"Master-data probe error for '{company_name}': {error_message}")
            recommended_actions.extend(_recommended_actions_for_error_kind(error_kind))
        elif master_rows == 0:
            warnings.append(f"Master-data probe returned zero groups/ledgers for '{company_name}'.")
            recommended_actions.append("Open the target company in Tally and leave it active before running `sync-masters`.")
    else:
        warnings.append("Voucher-type probe skipped because no company name was provided.")
        recommended_actions.append("Run `tally-db-pipeline list-companies` and then re-run with `--company`.")

    health_status = _derive_health_status(
        companies_ok=tests["companies"]["ok"],
        companies_error_kind=tests["companies"].get("error_kind"),
        voucher_tests=tests.get("voucher_types"),
        master_tests=tests.get("masters"),
    )

    return {
        "connected": True,
        "base_url": client.base_url,
        "companies": company_names,
        "company_families": company_families,
        "warnings": warnings,
        "health_status": health_status,
        "recommended_actions": _dedupe_preserve_order(recommended_actions),
        "tests": tests,
    }


def _recommended_actions_for_error_kind(error_kind: str | None) -> list[str]:
    if error_kind == "connection_error":
        return [
            "Verify Tally is running and HTTP/XML is enabled on the configured host and port.",
            "Verify the machine running this repo can reach the Tally machine over the network.",
        ]
    if error_kind == "timeout":
        return [
            "Make sure Tally is open and idle, then retry one command at a time.",
            "Increase `TALLY_TIMEOUT_SECONDS` for large probes or syncs.",
            "Prefer chunked commands for large historical ranges.",
        ]
    if error_kind == "line_error":
        return [
            "Check the exact company name and make sure it matches Tally exactly.",
            "Open the company in the Tally UI before retrying report-based commands.",
        ]
    if error_kind == "unexpected_error":
        return [
            "Capture a support bundle with `tally-db-pipeline support-bundle`.",
            "Inspect recent run errors with `tally-db-pipeline report`.",
        ]
    return []


def _health_status_for_error_kind(error_kind: str | None) -> str:
    if error_kind == "connection_error":
        return "unreachable"
    if error_kind == "timeout":
        return "reachable_but_stalled"
    if error_kind == "line_error":
        return "reachable_but_rejected"
    return "unhealthy"


def _derive_health_status(
    *,
    companies_ok: bool,
    companies_error_kind: str | None,
    voucher_tests: dict | None,
    master_tests: dict | None,
) -> str:
    if companies_error_kind:
        return _health_status_for_error_kind(companies_error_kind)
    if not companies_ok:
        return "reachable_but_no_companies"
    if voucher_tests and voucher_tests.get("error_kind"):
        return _health_status_for_error_kind(voucher_tests.get("error_kind"))
    if master_tests and master_tests.get("error_kind"):
        return _health_status_for_error_kind(master_tests.get("error_kind"))
    if master_tests and not master_tests.get("ok"):
        return "reachable_but_no_master_data"
    if voucher_tests and not voucher_tests.get("ok"):
        return "reachable_but_no_voucher_types"
    return "healthy"


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def build_bootstrap_plan(client: TallyClient, company_name: str | None = None) -> dict:
    discovery = discover_tally(client, company_name=company_name)
    resolved_company = company_name
    if not resolved_company and len(discovery["companies"]) == 1:
        resolved_company = discovery["companies"][0]

    inferred_start = infer_company_fiscal_year_start(resolved_company) if resolved_company else None
    plan: list[str] = []
    if not discovery["connected"]:
        plan.append("Fix connectivity or timeout issues before attempting sync commands.")
    else:
        plan.append("Run `tally-db-pipeline list-companies` to confirm the exact visible company names.")
        if discovery.get("company_families"):
            multi_year_families = [row for row in discovery["company_families"] if row["company_count"] > 1]
            if multi_year_families:
                plan.append("Run `tally-db-pipeline list-company-families` to inspect separate fiscal-year company variants.")
        if resolved_company:
            plan.append(f"Run `tally-db-pipeline doctor --company \"{resolved_company}\"` to confirm voucher and master-data access.")
            plan.append(f"Run `tally-db-pipeline sync-voucher-types --company \"{resolved_company}\"`.")
            if inferred_start:
                plan.append(
                    f"Run `tally-db-pipeline profile-vouchers-chunked --company \"{resolved_company}\" --from-date {inferred_start} --to-date {datetime.utcnow().strftime('%Y-%m-%d')} --chunk-days 31`."
                )
                plan.append(
                    f"Run `tally-db-pipeline sync-vouchers-incremental --company \"{resolved_company}\" --voucher-type Sales --chunk-days 31` after initial profile/testing."
                )
                resolved_meta = parse_company_name_metadata(resolved_company)
                if resolved_meta.get("has_fiscal_suffix"):
                    plan.append(
                        f"Run `tally-db-pipeline sync-company-family --selector \"{resolved_meta['stem']}\" --continue-on-error` to walk all visible fiscal-year company variants for that business."
                    )
            else:
                plan.append(
                    f"Run `tally-db-pipeline profile-vouchers --company \"{resolved_company}\" --from-date YYYY-MM-DD --to-date YYYY-MM-DD` once you know the useful date range."
                )
        else:
            plan.append("Pick one exact company name from `list-companies`, then re-run bootstrap with `--company`.")

    return {
        "base_url": client.base_url,
        "resolved_company": resolved_company,
        "inferred_start_date": inferred_start,
        "discovery": discovery,
        "plan": plan,
    }


def _discover_company_names(client: TallyClient) -> list[str]:
    payload = client.execute("companies", client.build_company_collection_xml())
    companies = parse_company_collection(payload["response_xml"])
    return [row["name"] for row in companies if row["name"]]


def list_company_families(client: TallyClient) -> dict:
    company_names = _discover_company_names(client)
    return {
        "base_url": client.base_url,
        "companies": company_names,
        "company_families": summarize_company_families(company_names),
    }


def _bounded_company_date_range(company_name: str) -> tuple[str | None, str | None]:
    start_date = infer_company_fiscal_year_start(company_name)
    end_date = datetime.utcnow().strftime("%Y-%m-%d")
    if start_date and start_date > end_date:
        end_date = start_date
    return start_date, end_date


def _summarize_window_results(results: list[dict]) -> dict:
    attempted = len(results)
    failed = sum(1 for row in results if row.get("error"))
    succeeded = attempted - failed
    zero_saved = sum(1 for row in results if not row.get("error") and row.get("saved", 0) == 0)
    return {
        "attempted": attempted,
        "succeeded": succeeded,
        "failed": failed,
        "zero_saved": zero_saved,
        "total_saved": sum(row.get("saved", 0) for row in results if not row.get("error")),
    }


def _summarize_profiled_sync_results(results: list[dict]) -> dict:
    attempted = len(results)
    failed = sum(1 for row in results if row.get("error"))
    succeeded = attempted - failed
    zero_saved = sum(1 for row in results if not row.get("error") and row.get("saved", 0) == 0)
    return {
        "attempted_voucher_types": attempted,
        "successful_voucher_types": succeeded,
        "failed_voucher_types": failed,
        "zero_saved_voucher_types": zero_saved,
        "total_saved": sum(row.get("saved", 0) for row in results if not row.get("error")),
    }


def _sync_master_collection(
    session: Session,
    client: TallyClient,
    *,
    run: SyncRun,
    request_type: str,
    collection_name: str,
    object_type: str,
    model,
    fields: list[str],
    company_name: str,
) -> list[dict]:
    payload = client.execute(
        request_type,
        client.build_collection_xml(collection_name, object_type, fields=fields, company=company_name),
    )
    _record_payload(session, run, payload, company_name=company_name)
    line_error = client.extract_line_error(payload["response_xml"])
    if line_error:
        raise RuntimeError(line_error)
    rows = parse_collection(payload["response_xml"], object_type)
    for row in rows:
        _upsert_by_name(session, model, row["name"], {k: v for k, v in row.items() if k != "name"}, company_name=company_name)
    return rows


def sync_masters(session: Session, client: TallyClient, company_name: str | None = None) -> dict:
    run = _start_run(session, "masters", company_name=company_name)
    try:
        resolved_company_name = company_name
        if not resolved_company_name:
            companies = sync_companies(session, client)
            names = [row["name"] for row in companies if row["name"]]
            if len(names) == 1:
                resolved_company_name = names[0]
            else:
                raise RuntimeError("Could not infer the target company. Pass an exact company name or make only one company visible in Tally.")

        if resolved_company_name:
            company = session.scalar(select(Company).where(Company.name == resolved_company_name))
            if company is None:
                company = Company(name=resolved_company_name)
                session.add(company)
            company.last_synced_at = datetime.utcnow()
            session.commit()

        groups = _sync_master_collection(
            session,
            client,
            run=run,
            request_type="groups",
            collection_name="Groups",
            object_type="Group",
            model=Group,
            fields=["Name", "Parent", "GUID", "IsRevenue", "IsDeemedPositive", "AffectsGrossProfit", "IsAddable"],
            company_name=resolved_company_name,
        )
        ledgers = _sync_master_collection(
            session,
            client,
            run=run,
            request_type="ledgers",
            collection_name="Ledgers",
            object_type="Ledger",
            model=Ledger,
            fields=[
                "Name",
                "Parent",
                "GUID",
                "OpeningBalance",
                "ClosingBalance",
                "MailingName",
                "Address",
                "LedStateName",
                "PriorStateName",
                "CountryOfResidence",
                "OldPINCode",
                "PINCode",
                "Email",
                "LedgerPhone",
                "LedgerMobile",
                "IncomeTaxNumber",
                "GSTRegistrationNumber",
                "PartyGSTIN",
                "GSTRegistrationType",
                "CurrencyName",
                "IsBillWiseOn",
                "AffectsStock",
                "CreatedBy",
            ],
            company_name=resolved_company_name,
        )

        if not groups and not ledgers:
            raise RuntimeError("No group or ledger master data returned from Tally collections. Open the target company in Tally and retry.")

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
                _upsert_by_name(session, model, row["name"], {k: v for k, v in row.items() if k != "name"}, company_name=resolved_company_name)

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
            _upsert_by_name(session, StockItem, balance["name"], {k: v for k, v in balance.items() if k != "name"}, company_name=resolved_company_name)

        _finish_run(session, run, "success")
        _upsert_checkpoint(
            session,
            entity_type="masters",
            company_name=resolved_company_name,
            status="success",
            row_count=len(groups) + len(ledgers),
        )
        return {
            "company": resolved_company_name,
            "groups": len(groups),
            "ledgers": len(ledgers),
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
            _upsert_by_name(session, VoucherType, row["name"], {k: v for k, v in row.items() if k != "name"}, company_name=company_name)
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
    range_mode: str = "daybook",
) -> dict:
    if range_mode not in {"auto", "daybook", "collection"}:
        raise ValueError("range_mode must be one of 'auto', 'daybook', or 'collection'.")
    run_label = f"vouchers:{voucher_type}"
    if from_date or to_date:
        run_label += ":range"
    run = _start_run(session, run_label, company_name=company_name)
    try:
        voucher_type_rows = _load_voucher_type_rows(session, company_name)
        effective_range_mode = range_mode
        if from_date or to_date:
            candidate_modes = ["daybook", "collection"] if range_mode == "auto" else [range_mode]
            last_range_error: Exception | None = None
            payload = None
            vouchers = None
            for candidate_mode in candidate_modes:
                effective_range_mode = candidate_mode
                if candidate_mode == "collection":
                    candidate_payload = client.execute(
                        "vouchers_collection_range",
                        client.build_voucher_type_collection_range_xml(
                            company_name,
                            voucher_type=voucher_type,
                            from_date=from_date,
                            to_date=to_date,
                        ),
                    )
                else:
                    candidate_payload = client.execute(
                        "vouchers_daybook",
                        client.build_daybook_xml(company_name, voucher_type=voucher_type, from_date=from_date, to_date=to_date),
                    )

                _record_payload(session, run, candidate_payload, company_name=company_name)
                line_error = client.extract_line_error(candidate_payload["response_xml"])
                if line_error:
                    raise RuntimeError(line_error)

                candidate_vouchers = parse_vouchers(candidate_payload["response_xml"])
                try:
                    _validate_voucher_dates_within_range(candidate_vouchers, from_date, to_date)
                except VoucherRangeValidationError as exc:
                    last_range_error = exc
                    if range_mode == "auto" and candidate_mode == "daybook":
                        continue
                    raise
                payload = candidate_payload
                vouchers = candidate_vouchers
                break

            if payload is None or vouchers is None:
                raise last_range_error or RuntimeError("No voucher payload could be validated.")
        else:
            payload = client.execute("vouchers", client.build_voucher_collection_xml(company_name, voucher_type))
            effective_range_mode = "full"
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
        return {"voucher_type": voucher_type, "saved": saved, "from_date": from_date, "to_date": to_date, "range_mode": effective_range_mode}
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
    range_mode: str = "daybook",
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
                    range_mode=range_mode,
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
                            range_mode=range_mode,
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
                            range_mode=range_mode,
                        )
                    )
                    continue
            result = {
                "voucher_type": voucher_type,
                "saved": 0,
                "from_date": from_date,
                "to_date": to_date,
                "error": str(exc),
                "range_mode": range_mode,
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
    range_mode: str = "daybook",
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
        range_mode=range_mode,
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

        voucher_type_rows = _load_voucher_type_rows(session, company_name)
        vouchers = parse_vouchers(payload["response_xml"])
        _validate_voucher_dates_within_range(vouchers, from_date, to_date)
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
    failed_windows = sum(1 for row in windows if row.get("error"))
    return {
        "company_name": company_name,
        "from_date": start_date,
        "to_date": end_date,
        "total_vouchers": sum(item["count"] for item in voucher_types),
        "voucher_types": voucher_types,
        "windows": windows,
        "window_summary": {
            "attempted": len(windows),
            "failed": failed_windows,
            "succeeded": len(windows) - failed_windows,
        },
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


def recommended_voucher_types_from_profile(
    profile_result: dict,
    *,
    include_standard: bool = True,
    include_custom: bool = True,
    min_count: int = 1,
) -> list[str]:
    recommended: list[str] = []
    seen: set[str] = set()
    for row in profile_result.get("voucher_types", []):
        base_type = row.get("base_voucher_type") or row.get("voucher_type_name")
        if row.get("count", 0) < min_count:
            continue
        is_standard = base_type in STANDARD_BASE_VOUCHER_TYPES
        if (is_standard and not include_standard) or (not is_standard and not include_custom):
            continue
        if base_type in seen:
            continue
        seen.add(base_type)
        recommended.append(base_type)
    return recommended


def sync_profiled_vouchers(
    session: Session,
    client: TallyClient,
    *,
    company_name: str,
    start_date: str,
    end_date: str,
    chunk_days: int = 31,
    include_standard: bool = True,
    include_custom: bool = True,
    min_count: int = 1,
    continue_on_error: bool = False,
    adaptive: bool = True,
    min_chunk_days: int = 1,
) -> dict:
    profile_result = profile_vouchers_in_chunks(
        session,
        client,
        company_name=company_name,
        start_date=start_date,
        end_date=end_date,
        chunk_days=chunk_days,
        adaptive=adaptive,
        min_chunk_days=min_chunk_days,
        continue_on_error=continue_on_error,
    )
    recommended_types = recommended_voucher_types_from_profile(
        profile_result,
        include_standard=include_standard,
        include_custom=include_custom,
        min_count=min_count,
    )

    results: list[dict] = []
    for voucher_type in recommended_types:
        try:
            chunk_results = sync_vouchers_in_chunks(
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
            results.append(
                {
                    "voucher_type": voucher_type,
                    "saved": sum(item.get("saved", 0) for item in chunk_results if not item.get("error")),
                    "windows": chunk_results,
                }
            )
        except Exception as exc:
            results.append({"voucher_type": voucher_type, "saved": 0, "error": str(exc), "windows": []})
            if not continue_on_error:
                raise

    return {
        "company_name": company_name,
        "from_date": start_date,
        "to_date": end_date,
        "recommended_voucher_types": recommended_types,
        "profile": profile_result,
        "results": results,
        "summary": _summarize_profiled_sync_results(results),
    }


def profile_company_family_vouchers(
    session: Session,
    client: TallyClient,
    *,
    selector: str,
    chunk_days: int = 31,
    adaptive: bool = True,
    min_chunk_days: int = 1,
    continue_on_error: bool = False,
) -> dict:
    company_names = _discover_company_names(client)
    family = resolve_company_family(company_names, selector)
    family.sort(key=lambda row: (row["start_year"] is None, row["start_year"] or 0, row["name"]))

    companies: list[dict] = []
    aggregate: dict[str, dict] = {}
    for company in family:
        from_date, to_date = _bounded_company_date_range(company["name"])
        if not from_date or not to_date:
            item = {
                "company_name": company["name"],
                "stem": company["stem"],
                "from_date": from_date,
                "to_date": to_date,
                "error": "Could not infer fiscal-year date range from company name. Use single-company profiling for this company.",
            }
            companies.append(item)
            if not continue_on_error:
                raise ValueError(item["error"])
            continue

        try:
            result = profile_vouchers_in_chunks(
                session,
                client,
                company_name=company["name"],
                start_date=from_date,
                end_date=to_date,
                chunk_days=chunk_days,
                adaptive=adaptive,
                min_chunk_days=min_chunk_days,
                continue_on_error=continue_on_error,
            )
            companies.append(result)
            for row in result["voucher_types"]:
                aggregate_row = aggregate.setdefault(
                    row["voucher_type_name"],
                    {
                        "voucher_type_name": row["voucher_type_name"],
                        "base_voucher_type": row["base_voucher_type"],
                        "count": 0,
                        "first_date": None,
                        "last_date": None,
                        "companies": [],
                    },
                )
                aggregate_row["count"] += row["count"]
                if row["first_date"] and (aggregate_row["first_date"] is None or row["first_date"] < aggregate_row["first_date"]):
                    aggregate_row["first_date"] = row["first_date"]
                if row["last_date"] and (aggregate_row["last_date"] is None or row["last_date"] > aggregate_row["last_date"]):
                    aggregate_row["last_date"] = row["last_date"]
                aggregate_row["companies"].append(
                    {
                        "company_name": company["name"],
                        "count": row["count"],
                        "first_date": row["first_date"],
                        "last_date": row["last_date"],
                    }
                )
        except Exception as exc:
            item = {
                "company_name": company["name"],
                "stem": company["stem"],
                "from_date": from_date,
                "to_date": to_date,
                "error": str(exc),
            }
            companies.append(item)
            if not continue_on_error:
                raise

    aggregate_voucher_types = sorted(aggregate.values(), key=lambda row: (-row["count"], row["voucher_type_name"]))
    successful_companies = [row for row in companies if not row.get("error")]
    return {
        "selector": selector,
        "family_stem": family[0]["stem"] if family else selector,
        "company_count": len(family),
        "companies": companies,
        "voucher_types": aggregate_voucher_types,
        "summary": {
            "attempted_companies": len(companies),
            "successful_companies": len(successful_companies),
            "failed_companies": sum(1 for row in companies if row.get("error")),
            "total_vouchers": sum(row.get("total_vouchers", 0) for row in successful_companies),
        },
    }


def sync_company_family(
    session: Session,
    client: TallyClient,
    *,
    selector: str,
    chunk_days: int = 31,
    include_standard: bool = True,
    include_custom: bool = True,
    min_count: int = 1,
    continue_on_error: bool = False,
    adaptive: bool = True,
    min_chunk_days: int = 1,
    sync_masters_for_each_company: bool = False,
) -> dict:
    company_names = _discover_company_names(client)
    family = resolve_company_family(company_names, selector)
    family.sort(key=lambda row: (row["start_year"] is None, row["start_year"] or 0, row["name"]))

    companies: list[dict] = []
    total_saved = 0
    for company in family:
        from_date, to_date = _bounded_company_date_range(company["name"])
        if not from_date or not to_date:
            item = {
                "company_name": company["name"],
                "stem": company["stem"],
                "error": "Could not infer fiscal-year date range from company name. Use single-company sync commands for this company.",
            }
            companies.append(item)
            if not continue_on_error:
                raise ValueError(item["error"])
            continue

        try:
            if sync_masters_for_each_company:
                sync_masters(session, client, company_name=company["name"])
            sync_voucher_types(session, client, company_name=company["name"])
            result = sync_profiled_vouchers(
                session,
                client,
                company_name=company["name"],
                start_date=from_date,
                end_date=to_date,
                chunk_days=chunk_days,
                include_standard=include_standard,
                include_custom=include_custom,
                min_count=min_count,
                continue_on_error=continue_on_error,
                adaptive=adaptive,
                min_chunk_days=min_chunk_days,
            )
            total_saved += result["summary"]["total_saved"]
            companies.append(result)
        except Exception as exc:
            item = {
                "company_name": company["name"],
                "stem": company["stem"],
                "from_date": from_date,
                "to_date": to_date,
                "error": str(exc),
            }
            companies.append(item)
            if not continue_on_error:
                raise

    successful_companies = [row for row in companies if not row.get("error")]
    return {
        "selector": selector,
        "family_stem": family[0]["stem"] if family else selector,
        "company_count": len(family),
        "companies": companies,
        "summary": {
            "attempted_companies": len(companies),
            "successful_companies": len(successful_companies),
            "failed_companies": sum(1 for row in companies if row.get("error")),
            "total_saved": total_saved,
        },
    }


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
                _upsert_by_name(session, Group, group["name"], {k: v for k, v in group.items() if k != "name"}, company_name=resolved_company)
            for ledger in parsed["ledgers"]:
                _upsert_by_name(session, Ledger, ledger["name"], {k: v for k, v in ledger.items() if k != "name"}, company_name=resolved_company)

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
                _upsert_by_name(session, model, row["name"], {k: v for k, v in row.items() if k != "name"}, company_name=company_name)
            _upsert_checkpoint(session, entity_type=f"replay:{kind}", company_name=company_name, status="success", row_count=len(rows))
            _finish_run(session, run, "success")
            return {"kind": kind, "count": len(rows)}

        if kind == "stock-item-balances":
            rows = parse_stock_item_balances(xml_text)
            for row in rows:
                _upsert_by_name(session, StockItem, row["name"], {k: v for k, v in row.items() if k != "name"}, company_name=company_name)
            _upsert_checkpoint(session, entity_type=f"replay:{kind}", company_name=company_name, status="success", row_count=len(rows))
            _finish_run(session, run, "success")
            return {"kind": kind, "count": len(rows)}

        if kind == "vouchers":
            if not company_name:
                raise RuntimeError("company_name is required for voucher replay.")
            voucher_type_rows = _load_voucher_type_rows(session, company_name)
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

    def blank_company_count(model) -> int:
        if not hasattr(model, "company_name"):
            return 0
        return session.scalar(select(func.count()).select_from(model).where(model.company_name == "")) or 0

    payload_oldest = session.scalar(select(func.min(RawPayload.created_at)))
    payload_newest = session.scalar(select(func.max(RawPayload.created_at)))

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
        "legacy_global_master_rows": {
            "groups": blank_company_count(Group),
            "ledgers": blank_company_count(Ledger),
            "stock_groups": blank_company_count(StockGroup),
            "stock_items": blank_company_count(StockItem),
            "units": blank_company_count(Unit),
            "godowns": blank_company_count(Godown),
            "cost_centres": blank_company_count(CostCentre),
            "voucher_types": blank_company_count(VoucherType),
        },
        "raw_payloads": count(RawPayload),
        "raw_payload_oldest": payload_oldest.isoformat(timespec="seconds") if payload_oldest else None,
        "raw_payload_newest": payload_newest.isoformat(timespec="seconds") if payload_newest else None,
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


def prune_legacy_global_master_rows(
    session: Session,
    *,
    dry_run: bool = False,
) -> dict:
    models = {
        "groups": Group,
        "ledgers": Ledger,
        "stock_groups": StockGroup,
        "stock_items": StockItem,
        "units": Unit,
        "godowns": Godown,
        "cost_centres": CostCentre,
        "voucher_types": VoucherType,
    }
    deleted: dict[str, int] = {}
    for key, model in models.items():
        rows = session.scalars(select(model).where(model.company_name == "")).all()
        deleted[key] = len(rows)
        if not dry_run:
            for row in rows:
                session.delete(row)
    if not dry_run:
        session.commit()
    return {"dry_run": dry_run, "deleted": deleted, "total_deleted": sum(deleted.values())}


def prune_raw_payloads(
    session: Session,
    *,
    keep_latest: int = 100,
    request_type: str | None = None,
    dry_run: bool = False,
) -> dict:
    if keep_latest < 0:
        raise ValueError("keep_latest must be zero or greater.")

    base_query = select(RawPayload.id).order_by(RawPayload.id.desc())
    if request_type:
        base_query = base_query.where(RawPayload.request_type == request_type)

    ids_to_keep = [row for row in session.scalars(base_query.limit(keep_latest)).all()]
    delete_query = select(RawPayload.id)
    if request_type:
        delete_query = delete_query.where(RawPayload.request_type == request_type)
    if ids_to_keep:
        delete_query = delete_query.where(RawPayload.id.not_in(ids_to_keep))

    ids_to_delete = [row for row in session.scalars(delete_query).all()]
    deleted_count = len(ids_to_delete)
    if deleted_count and not dry_run:
        session.execute(delete(RawPayload).where(RawPayload.id.in_(ids_to_delete)))
        session.commit()

    return {
        "request_type": request_type,
        "keep_latest": keep_latest,
        "dry_run": dry_run,
        "deleted_count": deleted_count,
        "kept_count": len(ids_to_keep),
    }


def create_support_bundle(
    session: Session,
    *,
    output_directory: str,
    include_payload_bodies: bool = False,
    redact_payload_bodies: bool = False,
    payload_limit: int = 5,
) -> dict:
    output_dir = Path(output_directory)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    bundle_dir = output_dir / f"tally-support-bundle-{timestamp}"
    bundle_dir.mkdir(parents=True, exist_ok=True)

    settings = get_settings()
    report = get_database_report(session)
    settings_snapshot = {
        "tally_host": settings.tally_host,
        "tally_port": settings.tally_port,
        "database_url": settings.database_url,
        "tally_timeout_seconds": settings.tally_timeout_seconds,
        "tally_request_delay_ms": settings.tally_request_delay_ms,
        "tally_max_retries": settings.tally_max_retries,
        "tally_retry_backoff_ms": settings.tally_retry_backoff_ms,
    }

    recent_payloads = []
    payload_rows = session.scalars(select(RawPayload).order_by(RawPayload.id.desc()).limit(payload_limit)).all()
    for row in payload_rows:
        item = {
            "id": row.id,
            "sync_run_id": row.sync_run_id,
            "request_type": row.request_type,
            "company_name": row.company_name,
            "response_sha256": row.response_sha256,
            "created_at": row.created_at.isoformat(timespec="seconds") if row.created_at else None,
            "request_length": len(row.request_xml or ""),
            "response_length": len(row.response_xml or ""),
        }
        if include_payload_bodies:
            request_xml = row.request_xml
            response_xml = row.response_xml
            if redact_payload_bodies:
                request_xml = _redact_xml(request_xml)
                response_xml = _redact_xml(response_xml)
            item["request_xml"] = request_xml
            item["response_xml"] = response_xml
        recent_payloads.append(item)

    (bundle_dir / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (bundle_dir / "settings.json").write_text(json.dumps(settings_snapshot, indent=2), encoding="utf-8")
    (bundle_dir / "recent_payloads.json").write_text(json.dumps(recent_payloads, indent=2), encoding="utf-8")
    (bundle_dir / "README.txt").write_text(
        "\n".join(
            [
                "Tally support bundle",
                "",
                "Files:",
                "- report.json: local database and sync status snapshot",
                "- settings.json: local runtime settings snapshot",
                "- recent_payloads.json: recent raw payload metadata",
            ]
            + (
                [
                    "- recent_payloads.json includes full request/response XML bodies"
                    + (" with basic redaction applied" if redact_payload_bodies else "")
                ]
                if include_payload_bodies
                else []
            )
        )
        + "\n",
        encoding="utf-8",
    )

    return {
        "bundle_directory": str(bundle_dir),
        "files": ["README.txt", "report.json", "settings.json", "recent_payloads.json"],
        "recent_payload_count": len(recent_payloads),
        "include_payload_bodies": include_payload_bodies,
        "redact_payload_bodies": redact_payload_bodies,
    }


def _redact_xml(text: str) -> str:
    patterns = [
        (r"(<(PARTYGSTIN|GSTREGISTRATIONNUMBER|GSTN)>)(.*?)(</\2>)", r"\1[REDACTED]\4"),
        (r"(<(INCOMETAXNUMBER|PAN|PANNUMBER)>)(.*?)(</\2>)", r"\1[REDACTED]\4"),
        (r"(<(EMAIL)>)(.*?)(</\2>)", r"\1[REDACTED]\4"),
        (r"(<(PHONE|LEDGERPHONE|LEDGERMOBILE)>)(.*?)(</\2>)", r"\1[REDACTED]\4"),
        (r"(<(VOUCHERNUMBER)>)(.*?)(</\2>)", r"\1[REDACTED]\4"),
        (r"(<(PARTYLEDGERNAME|PARTYNAME|LEDGERNAME|STOCKITEMNAME)>)(.*?)(</\2>)", r"\1[REDACTED]\4"),
    ]
    redacted = text
    for pattern, replacement in patterns:
        redacted = re.sub(pattern, replacement, redacted, flags=re.IGNORECASE | re.DOTALL)
    return redacted


def count_running_syncs(session: Session) -> int:
    return session.scalar(select(func.count()).select_from(SyncRun).where(SyncRun.status == "running")) or 0
