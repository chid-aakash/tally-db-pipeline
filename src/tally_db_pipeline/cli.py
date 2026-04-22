from __future__ import annotations

import json
from typing import Optional

import typer

from .config import get_settings
from .db import get_session
from .parsers import parse_company_collection
from .sync import (
    STANDARD_VOUCHER_TYPES,
    discover_tally,
    get_database_report,
    init_db,
    sync_companies,
    sync_masters,
    sync_standard_vouchers,
    sync_voucher_types,
    sync_vouchers,
)
from .tally_client import TallyClient


app = typer.Typer(no_args_is_help=True, add_completion=False)


def _client() -> TallyClient:
    settings = get_settings()
    return TallyClient(
        host=settings.tally_host,
        port=settings.tally_port,
        timeout=settings.tally_timeout_seconds,
        request_delay_ms=settings.tally_request_delay_ms,
        max_retries=settings.tally_max_retries,
        retry_backoff_ms=settings.tally_retry_backoff_ms,
    )


@app.command("init-db")
def init_db_command() -> None:
    init_db()
    typer.echo("Database schema initialized.")


@app.command("ping")
def ping() -> None:
    client = _client()
    result = client.test_connection()
    if not result.get("connected"):
        typer.echo(result.get("error", "Connection failed"), err=True)
        raise typer.Exit(code=1)
    companies = parse_company_collection(result["response_xml"])
    typer.echo(f"Connected to Tally at {client.base_url}")
    typer.echo(f"Companies visible: {len(companies)}")
    for row in companies:
        if row["name"]:
            typer.echo(f"- {row['name']}")


@app.command("list-companies")
def list_companies() -> None:
    client = _client()
    result = client.execute("companies", client.build_company_collection_xml())
    companies = parse_company_collection(result["response_xml"])
    for row in companies:
        if row["name"]:
            typer.echo(row["name"])


@app.command("discover")
def discover(company: Optional[str] = typer.Option(default=None, help="Exact company name to probe voucher-type access.")) -> None:
    result = discover_tally(_client(), company_name=company)
    typer.echo(json.dumps(result, indent=2))


@app.command("doctor")
def doctor(company: Optional[str] = typer.Option(default=None, help="Exact company name for voucher-type diagnostics.")) -> None:
    result = discover_tally(_client(), company_name=company)
    if not result["connected"]:
        typer.echo(f"FAIL: {result['warnings'][0]}", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"Connected to {result['base_url']}")
    typer.echo(f"Companies discovered: {len(result['companies'])}")
    for name in result["companies"]:
        typer.echo(f"- {name}")

    companies_test = result["tests"].get("companies", {})
    typer.echo(f"Company discovery ok: {companies_test.get('ok', False)}")
    typer.echo(f"Company discovery count: {companies_test.get('count', 0)}")

    voucher_types = result["tests"].get("voucher_types")
    if voucher_types is not None:
        typer.echo(f"Voucher-type probe ok: {voucher_types.get('ok', False)}")
        typer.echo(f"Voucher-type count: {voucher_types.get('count', 0)}")
        if voucher_types.get("error"):
            typer.echo(f"Voucher-type error: {voucher_types['error']}")

    if result["warnings"]:
        typer.echo("Warnings:")
        for warning in result["warnings"]:
            typer.echo(f"- {warning}")


@app.command("sync-companies")
def sync_companies_command() -> None:
    init_db()
    with get_session() as session:
        rows = sync_companies(session, _client())
    typer.echo(f"Synced {len(rows)} companies.")


@app.command("sync-masters")
def sync_masters_command() -> None:
    init_db()
    with get_session() as session:
        result = sync_masters(session, _client())
    typer.echo(f"Synced masters for company: {result.get('company') or 'unknown'}")
    typer.echo(f"Groups: {result['groups']}")
    typer.echo(f"Ledgers: {result['ledgers']}")


@app.command("sync-voucher-types")
def sync_voucher_types_command(company: Optional[str] = typer.Option(default=None, help="Exact Tally company name if needed.")) -> None:
    init_db()
    with get_session() as session:
        rows = sync_voucher_types(session, _client(), company_name=company)
    typer.echo(f"Synced {len(rows)} voucher types.")


@app.command("sync-vouchers")
def sync_vouchers_command(
    company: str = typer.Option(..., help="Exact Tally company name, including FY suffix where applicable."),
    voucher_type: str = typer.Option(..., help="Base voucher type, for example Sales, Purchase, Receipt, Payment."),
) -> None:
    init_db()
    with get_session() as session:
        result = sync_vouchers(session, _client(), company_name=company, voucher_type=voucher_type)
    typer.echo(f"Synced {result['saved']} vouchers for type: {result['voucher_type']}")


@app.command("sync-standard-vouchers")
def sync_standard_vouchers_command(
    company: str = typer.Option(..., help="Exact Tally company name, including FY suffix where applicable."),
    continue_on_error: bool = typer.Option(False, help="Continue even if one voucher family fails."),
) -> None:
    init_db()
    with get_session() as session:
        results = sync_standard_vouchers(
            session,
            _client(),
            company_name=company,
            continue_on_error=continue_on_error,
        )
    for result in results:
        if result.get("error"):
            typer.echo(f"{result['voucher_type']}: ERROR - {result['error']}")
        else:
            typer.echo(f"{result['voucher_type']}: {result['saved']}")


@app.command("sync-all")
def sync_all(
    company: str = typer.Option(..., help="Exact Tally company name, including FY suffix where applicable."),
    continue_on_error: bool = typer.Option(False, help="Continue even if one voucher family fails."),
) -> None:
    init_db()
    client = _client()
    with get_session() as session:
        sync_companies(session, client)
        sync_masters(session, client)
        sync_voucher_types(session, client, company_name=company)
        results = sync_standard_vouchers(
            session,
            client,
            company_name=company,
            continue_on_error=continue_on_error,
        )
    for result in results:
        if result.get("error"):
            typer.echo(f"{result['voucher_type']}: ERROR - {result['error']}")
        else:
            typer.echo(f"{result['voucher_type']}: {result['saved']}")


@app.command("report")
def report() -> None:
    init_db()
    with get_session() as session:
        result = get_database_report(session)
    typer.echo(json.dumps(result, indent=2))
