from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime

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
    SyncRun,
    Unit,
    Voucher,
    VoucherInventoryEntry,
    VoucherLedgerEntry,
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
        _finish_run(session, run, "success")
        return companies
    except Exception as exc:
        session.rollback()
        _finish_run(session, run, "failed", str(exc))
        raise


def discover_tally(client: TallyClient, company_name: str | None = None) -> dict:
    connection = client.test_connection()
    if not connection.get("connected"):
        return {
            "connected": False,
            "base_url": client.base_url,
            "companies": [],
            "warnings": [connection.get("error", "Connection failed")],
            "tests": {},
        }

    companies = parse_company_collection(connection["response_xml"])
    company_names = [row["name"] for row in companies if row["name"]]
    warnings: list[str] = []
    tests: dict[str, dict] = {}

    tests["companies"] = {
        "ok": bool(company_names),
        "count": len(company_names),
    }
    if not company_names:
        warnings.append("No companies were discoverable from Tally. The active company may not be open or exposed.")

    if company_name:
        voucher_type_payload = client.execute(
            "voucher_types_probe",
            client.build_collection_xml(
                "VoucherTypes",
                "Voucher Type",
                fields=["Name", "Parent", "NumberingMethod"],
                company=company_name,
            ),
        )
        voucher_type_error = client.extract_line_error(voucher_type_payload["response_xml"])
        voucher_type_rows = parse_collection(voucher_type_payload["response_xml"], "Voucher Type")
        tests["voucher_types"] = {
            "ok": bool(voucher_type_rows),
            "error": voucher_type_error,
            "count": len(voucher_type_rows),
        }
        if voucher_type_error:
            warnings.append(f"Voucher type probe error for '{company_name}': {voucher_type_error}")
        elif not voucher_type_rows:
            warnings.append(f"No voucher types returned for '{company_name}'.")
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
        return {
            "company": resolved_company_name,
            "groups": len(accounts["groups"]),
            "ledgers": len(accounts["ledgers"]),
        }
    except Exception as exc:
        session.rollback()
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
        _finish_run(session, run, "success")
        return rows
    except Exception as exc:
        session.rollback()
        _finish_run(session, run, "failed", str(exc))
        raise


def sync_vouchers(session: Session, client: TallyClient, company_name: str, voucher_type: str) -> dict:
    run = _start_run(session, f"vouchers:{voucher_type}", company_name=company_name)
    try:
        voucher_type_rows = [
            {"name": row.name, "parent": row.parent, "numbering_method": row.numbering_method}
            for row in session.scalars(select(VoucherType)).all()
        ]
        payload = client.execute("vouchers", client.build_voucher_collection_xml(company_name, voucher_type))
        _record_payload(session, run, payload, company_name=company_name)
        line_error = client.extract_line_error(payload["response_xml"])
        if line_error:
            raise RuntimeError(line_error)
        vouchers = parse_vouchers(payload["response_xml"])

        saved = 0
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

            saved += 1

        session.commit()
        _finish_run(session, run, "success")
        return {"voucher_type": voucher_type, "saved": saved}
    except Exception as exc:
        session.rollback()
        _finish_run(session, run, "failed", str(exc))
        raise


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
            _finish_run(session, run, "success")
            return {"kind": kind, "count": len(rows)}

        if kind == "stock-item-balances":
            rows = parse_stock_item_balances(xml_text)
            for row in rows:
                _upsert_by_name(session, StockItem, row["name"], {k: v for k, v in row.items() if k != "name"})
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
                saved += 1
            session.commit()
            _finish_run(session, run, "success")
            return {"kind": kind, "saved": saved}

        raise ValueError(f"Unsupported replay kind: {kind}")
    except Exception as exc:
        session.rollback()
        _finish_run(session, run, "failed", str(exc))
        raise


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
        "raw_payloads": count(RawPayload),
        "running_syncs": count_running_syncs(session),
        "latest_voucher_syncs": latest_voucher_syncs,
        "recent_runs": recent_runs,
    }


def count_running_syncs(session: Session) -> int:
    return session.scalar(select(func.count()).select_from(SyncRun).where(SyncRun.status == "running")) or 0
