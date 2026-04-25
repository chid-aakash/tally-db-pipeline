from __future__ import annotations

import json
from contextlib import contextmanager
from functools import wraps
from typing import Optional

import typer

from .config import get_settings
from .db import get_session
from .parsers import parse_company_collection
from .sync import (
    STANDARD_VOUCHER_TYPES,
    build_bootstrap_plan,
    create_support_bundle,
    discover_tally,
    get_database_report,
    init_db,
    list_company_families,
    materialize_voucher_headers_from_latest_profile_payload,
    prune_raw_payloads,
    prune_legacy_global_master_rows,
    profile_company_family_vouchers,
    profile_vouchers,
    profile_vouchers_in_chunks,
    replay_xml_file,
    replay_xml_bundle,
    sync_companies,
    sync_company_family,
    sync_masters,
    sync_standard_vouchers,
    sync_profiled_vouchers,
    sync_voucher_details_by_guid,
    sync_voucher_details_by_master_id,
    sync_voucher_details_from_headers_batched,
    sync_voucher_details_from_headers,
    sync_voucher_headers,
    sync_voucher_types,
    sync_vouchers,
    sync_vouchers_in_chunks,
    sync_vouchers_incremental,
)
from .tally_client import TallyClient


app = typer.Typer(no_args_is_help=True, add_completion=False)


def _emit_chunk_progress(event: dict) -> None:
    if event["event"] == "start":
        typer.echo(f"{event['from_date']}..{event['to_date']}: START")
        return
    if event["event"] == "success":
        line = f"{event['from_date']}..{event['to_date']}: {event['saved']}"
        matched_voucher_types = event.get("matched_voucher_types") or []
        if matched_voucher_types:
            line += f" [{', '.join(matched_voucher_types)}]"
        typer.echo(line)
        return
    if event["event"] == "error":
        typer.echo(f"{event['from_date']}..{event['to_date']}: ERROR - {event['error']}")


def command(name: str):
    def decorator(func):
        @app.command(name)
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except typer.Exit:
                raise
            except Exception as exc:
                typer.echo(str(exc), err=True)
                raise typer.Exit(code=1)

        return wrapper

    return decorator


def _client() -> TallyClient:
    settings = get_settings()
    return TallyClient(
        host=settings.tally_host,
        port=settings.tally_port,
        timeout=settings.tally_timeout_seconds,
        request_delay_ms=settings.tally_request_delay_ms,
        max_retries=settings.tally_max_retries,
        retry_backoff_ms=settings.tally_retry_backoff_ms,
        lock_file=settings.tally_lock_file,
        lock_stale_seconds=settings.tally_lock_stale_seconds,
    )


@contextmanager
def _tally_client():
    client = _client()
    try:
        with client as locked_client:
            yield locked_client
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1)


@command("init-db")
def init_db_command() -> None:
    init_db()
    typer.echo("Database schema initialized.")


@command("ping")
def ping() -> None:
    with _tally_client() as client:
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


@command("list-companies")
def list_companies() -> None:
    with _tally_client() as client:
        result = client.execute("companies", client.build_company_collection_xml())
    companies = parse_company_collection(result["response_xml"])
    for row in companies:
        if row["name"]:
            typer.echo(row["name"])


@command("list-company-families")
def list_company_families_command() -> None:
    with _tally_client() as client:
        result = list_company_families(client)
    typer.echo(json.dumps(result, indent=2))


@command("discover")
def discover(company: Optional[str] = typer.Option(default=None, help="Exact company name to probe voucher-type access.")) -> None:
    with _tally_client() as client:
        result = discover_tally(client, company_name=company)
    typer.echo(json.dumps(result, indent=2))


@command("doctor")
def doctor(company: Optional[str] = typer.Option(default=None, help="Exact company name for voucher-type diagnostics.")) -> None:
    with _tally_client() as client:
        result = discover_tally(client, company_name=company)
    if not result["connected"]:
        companies_test = result["tests"].get("companies", {})
        typer.echo(f"FAIL: {result['warnings'][0]}", err=True)
        typer.echo(f"Health status: {result.get('health_status')}", err=True)
        if companies_test:
            typer.echo(f"Company probe error kind: {companies_test.get('error_kind')}", err=True)
            typer.echo(f"Company probe duration ms: {companies_test.get('duration_ms')}", err=True)
        for action in result.get("recommended_actions", []):
            typer.echo(f"Recommended: {action}", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"Connected to {result['base_url']}")
    typer.echo(f"Health status: {result.get('health_status')}")
    typer.echo(f"Companies discovered: {len(result['companies'])}")
    for name in result["companies"]:
        typer.echo(f"- {name}")

    companies_test = result["tests"].get("companies", {})
    typer.echo(f"Company discovery ok: {companies_test.get('ok', False)}")
    typer.echo(f"Company discovery count: {companies_test.get('count', 0)}")
    if companies_test.get("duration_ms") is not None:
        typer.echo(f"Company discovery duration ms: {companies_test.get('duration_ms')}")
    if companies_test.get("error_kind"):
        typer.echo(f"Company discovery error kind: {companies_test.get('error_kind')}")

    voucher_types = result["tests"].get("voucher_types")
    if voucher_types is not None:
        typer.echo(f"Voucher-type probe ok: {voucher_types.get('ok', False)}")
        typer.echo(f"Voucher-type count: {voucher_types.get('count', 0)}")
        if voucher_types.get("duration_ms") is not None:
            typer.echo(f"Voucher-type probe duration ms: {voucher_types.get('duration_ms')}")
        if voucher_types.get("error_kind"):
            typer.echo(f"Voucher-type probe error kind: {voucher_types.get('error_kind')}")
        if voucher_types.get("error"):
            typer.echo(f"Voucher-type error: {voucher_types['error']}")

    masters = result["tests"].get("masters")
    if masters is not None:
        typer.echo(f"Master-data probe ok: {masters.get('ok', False)}")
        typer.echo(f"Master-data group count: {masters.get('group_count', 0)}")
        typer.echo(f"Master-data ledger count: {masters.get('ledger_count', 0)}")
        if masters.get("duration_ms") is not None:
            typer.echo(f"Master-data probe duration ms: {masters.get('duration_ms')}")
        if masters.get("error_kind"):
            typer.echo(f"Master-data probe error kind: {masters.get('error_kind')}")
        if masters.get("error"):
            typer.echo(f"Master-data error: {masters['error']}")

    if result["warnings"]:
        typer.echo("Warnings:")
        for warning in result["warnings"]:
            typer.echo(f"- {warning}")
    if result.get("recommended_actions"):
        typer.echo("Recommended actions:")
        for action in result["recommended_actions"]:
            typer.echo(f"- {action}")


@command("bootstrap")
def bootstrap(company: Optional[str] = typer.Option(default=None, help="Exact company name if already known.")) -> None:
    with _tally_client() as client:
        result = build_bootstrap_plan(client, company_name=company)
    typer.echo(json.dumps(result, indent=2))


@command("sync-companies")
def sync_companies_command() -> None:
    init_db()
    with _tally_client() as client, get_session() as session:
        rows = sync_companies(session, client)
    typer.echo(f"Synced {len(rows)} companies.")


@command("sync-masters")
def sync_masters_command() -> None:
    init_db()
    with _tally_client() as client, get_session() as session:
        result = sync_masters(session, client)
    typer.echo(f"Synced masters for company: {result.get('company') or 'unknown'}")
    typer.echo(f"Groups: {result['groups']}")
    typer.echo(f"Ledgers: {result['ledgers']}")


@command("sync-voucher-types")
def sync_voucher_types_command(company: Optional[str] = typer.Option(default=None, help="Exact Tally company name if needed.")) -> None:
    init_db()
    with _tally_client() as client, get_session() as session:
        rows = sync_voucher_types(session, client, company_name=company)
    typer.echo(f"Synced {len(rows)} voucher types.")


@command("sync-vouchers")
def sync_vouchers_command(
    company: str = typer.Option(..., help="Exact Tally company name, including FY suffix where applicable."),
    voucher_type: str = typer.Option(..., help="Base voucher type, for example Sales, Purchase, Receipt, Payment."),
    from_date: Optional[str] = typer.Option(default=None, help="Inclusive start date in YYYY-MM-DD format."),
    to_date: Optional[str] = typer.Option(default=None, help="Inclusive end date in YYYY-MM-DD format."),
    range_mode: str = typer.Option("collection", help="Range export strategy for dated voucher pulls: collection or daybook."),
) -> None:
    init_db()
    with _tally_client() as client, get_session() as session:
        result = sync_vouchers(
            session,
            client,
            company_name=company,
            voucher_type=voucher_type,
            from_date=from_date,
            to_date=to_date,
            range_mode=range_mode,
        )
    typer.echo(f"Synced {result['saved']} vouchers for type: {result['voucher_type']}")
    matched_voucher_types = result.get("matched_voucher_types") or []
    if matched_voucher_types:
        typer.echo(f"Matched exact voucher types: {', '.join(matched_voucher_types)}")
    if result.get("from_date") or result.get("to_date"):
        typer.echo(f"Date range: {result.get('from_date') or result.get('to_date')} to {result.get('to_date') or result.get('from_date')}")


@command("sync-voucher-headers")
def sync_voucher_headers_command(
    company: str = typer.Option(..., help="Exact Tally company name, including FY suffix where applicable."),
    from_date: str = typer.Option(..., help="Inclusive start date in YYYY-MM-DD format."),
    to_date: str = typer.Option(..., help="Inclusive end date in YYYY-MM-DD format."),
) -> None:
    init_db()
    with _tally_client() as client, get_session() as session:
        result = sync_voucher_headers(
            session,
            client,
            company_name=company,
            from_date=from_date,
            to_date=to_date,
        )
    typer.echo(f"Synced {result['saved']} voucher headers.")
    typer.echo(json.dumps(result["voucher_types"], indent=2, sort_keys=True))


@command("materialize-voucher-headers")
def materialize_voucher_headers_command(
    company: str = typer.Option(..., help="Exact Tally company name, including FY suffix where applicable."),
) -> None:
    init_db()
    with get_session() as session:
        result = materialize_voucher_headers_from_latest_profile_payload(session, company_name=company)
    typer.echo(f"Materialized {result['saved']} voucher headers from payload {result['source_payload_id']}.")
    typer.echo(json.dumps(result["voucher_types"], indent=2, sort_keys=True))


@command("sync-voucher-detail")
def sync_voucher_detail_command(
    company: str = typer.Option(..., help="Exact Tally company name, including FY suffix where applicable."),
    guid: str = typer.Option(..., help="Exact Tally voucher GUID to fetch."),
) -> None:
    init_db()
    with _tally_client() as client, get_session() as session:
        result = sync_voucher_details_by_guid(session, client, company_name=company, guid=guid)
    typer.echo(json.dumps(result, indent=2, sort_keys=True))


@command("sync-voucher-detail-master-id")
def sync_voucher_detail_master_id_command(
    company: str = typer.Option(..., help="Exact Tally company name, including FY suffix where applicable."),
    master_id: str = typer.Option(..., help="Exact Tally voucher MASTERID to fetch."),
) -> None:
    init_db()
    with _tally_client() as client, get_session() as session:
        result = sync_voucher_details_by_master_id(session, client, company_name=company, master_id=master_id)
    typer.echo(json.dumps(result, indent=2, sort_keys=True))


@command("sync-voucher-details")
def sync_voucher_details_command(
    company: str = typer.Option(..., help="Exact Tally company name, including FY suffix where applicable."),
    voucher_type: Optional[str] = typer.Option(default=None, help="Exact staged Tally voucher type name, for example Temporary Purchase."),
    limit: int = typer.Option(10, help="Maximum voucher detail records to fetch in this run."),
    only_missing_detail: bool = typer.Option(True, help="Only fetch vouchers with no staged ledger, inventory, or unknown-section detail."),
    continue_on_error: bool = typer.Option(False, help="Continue after a per-voucher detail failure."),
) -> None:
    init_db()
    with _tally_client() as client, get_session() as session:
        result = sync_voucher_details_from_headers(
            session,
            client,
            company_name=company,
            voucher_type=voucher_type,
            limit=limit,
            only_missing_detail=only_missing_detail,
            continue_on_error=continue_on_error,
        )
    typer.echo(json.dumps(result, indent=2, sort_keys=True))


@command("sync-voucher-details-batched")
def sync_voucher_details_batched_command(
    company: str = typer.Option(..., help="Exact Tally company name, including FY suffix where applicable."),
    voucher_type: Optional[str] = typer.Option(default=None, help="Exact staged Tally voucher type name, for example Temporary Purchase."),
    limit: int = typer.Option(10, help="Maximum missing voucher detail records to fetch in this run."),
    batch_size: int = typer.Option(1, help="Number of MASTERIDs to request per bounded batch. Use 1 for the proven safe object-export path."),
    progress_every: int = typer.Option(50, help="Emit one progress line after this many successful vouchers."),
    continue_on_error: bool = typer.Option(False, help="Continue after a per-batch or per-voucher detail failure."),
    fallback_to_single: bool = typer.Option(True, help="Fall back to proven single MASTERID object export if a batch is rejected."),
) -> None:
    init_db()

    def emit(event: dict) -> None:
        typer.echo(json.dumps(event, sort_keys=True))

    with _tally_client() as client, get_session() as session:
        result = sync_voucher_details_from_headers_batched(
            session,
            client,
            company_name=company,
            voucher_type=voucher_type,
            limit=limit,
            batch_size=batch_size,
            progress_every=progress_every,
            continue_on_error=continue_on_error,
            fallback_to_single=fallback_to_single,
            progress_callback=emit,
        )
    typer.echo(json.dumps(result, indent=2, sort_keys=True))


@command("sync-vouchers-chunked")
def sync_vouchers_chunked_command(
    company: str = typer.Option(..., help="Exact Tally company name, including FY suffix where applicable."),
    voucher_type: str = typer.Option(..., help="Base voucher type, for example Sales, Purchase, Receipt, Payment."),
    from_date: str = typer.Option(..., help="Inclusive start date in YYYY-MM-DD format."),
    to_date: str = typer.Option(..., help="Inclusive end date in YYYY-MM-DD format."),
    chunk_days: int = typer.Option(31, help="Chunk size in days."),
    continue_on_error: bool = typer.Option(False, help="Continue even if one date window fails."),
    adaptive: bool = typer.Option(True, help="Automatically split failed date windows into smaller windows."),
    min_chunk_days: int = typer.Option(1, help="Smallest window size to try when adaptive splitting is enabled."),
    range_mode: str = typer.Option("collection", help="Range export strategy for dated voucher pulls: collection or daybook."),
    newest_first: bool = typer.Option(True, help="Process the newest windows first so recent data lands before older history."),
) -> None:
    init_db()
    with _tally_client() as client, get_session() as session:
        results = sync_vouchers_in_chunks(
            session,
            client,
            company_name=company,
            voucher_type=voucher_type,
            start_date=from_date,
            end_date=to_date,
            chunk_days=chunk_days,
            continue_on_error=continue_on_error,
            adaptive=adaptive,
            min_chunk_days=min_chunk_days,
            range_mode=range_mode,
            newest_first=newest_first,
            progress_callback=_emit_chunk_progress,
        )
    if not results:
        typer.echo("No windows were processed.")


@command("sync-vouchers-incremental")
def sync_vouchers_incremental_command(
    company: str = typer.Option(..., help="Exact Tally company name, including FY suffix where applicable."),
    voucher_type: str = typer.Option(..., help="Base voucher type, for example Sales, Purchase, Receipt, Payment."),
    since_date: Optional[str] = typer.Option(default=None, help="Initial inclusive start date in YYYY-MM-DD format when no checkpoint exists."),
    until_date: Optional[str] = typer.Option(default=None, help="Inclusive end date in YYYY-MM-DD format. Defaults to today."),
    chunk_days: int = typer.Option(31, help="Chunk size in days."),
    continue_on_error: bool = typer.Option(False, help="Continue even if one date window fails."),
    adaptive: bool = typer.Option(True, help="Automatically split failed date windows into smaller windows."),
    min_chunk_days: int = typer.Option(1, help="Smallest window size to try when adaptive splitting is enabled."),
    range_mode: str = typer.Option("collection", help="Range export strategy for dated voucher pulls: collection or daybook."),
    newest_first: bool = typer.Option(True, help="Process the newest windows first so recent data lands before older history."),
) -> None:
    init_db()
    with _tally_client() as client, get_session() as session:
        results = sync_vouchers_incremental(
            session,
            client,
            company_name=company,
            voucher_type=voucher_type,
            since_date=since_date,
            until_date=until_date,
            chunk_days=chunk_days,
            continue_on_error=continue_on_error,
            adaptive=adaptive,
            min_chunk_days=min_chunk_days,
            range_mode=range_mode,
            newest_first=newest_first,
            progress_callback=_emit_chunk_progress,
        )
    if not results:
        typer.echo("No windows were processed.")


@command("profile-vouchers")
def profile_vouchers_command(
    company: str = typer.Option(..., help="Exact Tally company name, including FY suffix where applicable."),
    from_date: str = typer.Option(..., help="Inclusive start date in YYYY-MM-DD format."),
    to_date: str = typer.Option(..., help="Inclusive end date in YYYY-MM-DD format."),
) -> None:
    init_db()
    with _tally_client() as client, get_session() as session:
        result = profile_vouchers(
            session,
            client,
            company_name=company,
            from_date=from_date,
            to_date=to_date,
        )
    typer.echo(json.dumps(result, indent=2))


@command("profile-vouchers-chunked")
def profile_vouchers_chunked_command(
    company: str = typer.Option(..., help="Exact Tally company name, including FY suffix where applicable."),
    from_date: str = typer.Option(..., help="Inclusive start date in YYYY-MM-DD format."),
    to_date: str = typer.Option(..., help="Inclusive end date in YYYY-MM-DD format."),
    chunk_days: int = typer.Option(31, help="Chunk size in days."),
    adaptive: bool = typer.Option(True, help="Automatically split failed date windows into smaller windows."),
    min_chunk_days: int = typer.Option(1, help="Smallest window size to try when adaptive splitting is enabled."),
    continue_on_error: bool = typer.Option(False, help="Continue even if one date window fails."),
) -> None:
    init_db()
    with _tally_client() as client, get_session() as session:
        result = profile_vouchers_in_chunks(
            session,
            client,
            company_name=company,
            start_date=from_date,
            end_date=to_date,
            chunk_days=chunk_days,
            adaptive=adaptive,
            min_chunk_days=min_chunk_days,
            continue_on_error=continue_on_error,
        )
    typer.echo(json.dumps(result, indent=2))


@command("sync-standard-vouchers")
def sync_standard_vouchers_command(
    company: str = typer.Option(..., help="Exact Tally company name, including FY suffix where applicable."),
    continue_on_error: bool = typer.Option(False, help="Continue even if one voucher family fails."),
) -> None:
    init_db()
    with _tally_client() as client, get_session() as session:
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


@command("sync-profiled-vouchers")
def sync_profiled_vouchers_command(
    company: str = typer.Option(..., help="Exact Tally company name, including FY suffix where applicable."),
    from_date: str = typer.Option(..., help="Inclusive start date in YYYY-MM-DD format."),
    to_date: str = typer.Option(..., help="Inclusive end date in YYYY-MM-DD format."),
    chunk_days: int = typer.Option(31, help="Chunk size in days."),
    include_standard: bool = typer.Option(True, help="Include standard voucher base types."),
    include_custom: bool = typer.Option(True, help="Include custom or non-standard voucher base types."),
    min_count: int = typer.Option(1, help="Only sync profiled voucher types seen at least this many times."),
    continue_on_error: bool = typer.Option(False, help="Continue even if one profiled voucher type fails."),
    adaptive: bool = typer.Option(True, help="Automatically split failed date windows into smaller windows."),
    min_chunk_days: int = typer.Option(1, help="Smallest window size to try when adaptive splitting is enabled."),
) -> None:
    init_db()
    with _tally_client() as client, get_session() as session:
        result = sync_profiled_vouchers(
            session,
            client,
            company_name=company,
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
    typer.echo(json.dumps(result, indent=2))


@command("profile-company-family")
def profile_company_family_command(
    selector: str = typer.Option(..., help="Either an exact company name or the shared business stem without the FY suffix."),
    chunk_days: int = typer.Option(31, help="Chunk size in days."),
    adaptive: bool = typer.Option(True, help="Automatically split failed date windows into smaller windows."),
    min_chunk_days: int = typer.Option(1, help="Smallest window size to try when adaptive splitting is enabled."),
    continue_on_error: bool = typer.Option(False, help="Continue even if one company or date window fails."),
) -> None:
    init_db()
    with _tally_client() as client, get_session() as session:
        result = profile_company_family_vouchers(
            session,
            client,
            selector=selector,
            chunk_days=chunk_days,
            adaptive=adaptive,
            min_chunk_days=min_chunk_days,
            continue_on_error=continue_on_error,
        )
    typer.echo(json.dumps(result, indent=2))


@command("sync-company-family")
def sync_company_family_command(
    selector: str = typer.Option(..., help="Either an exact company name or the shared business stem without the FY suffix."),
    chunk_days: int = typer.Option(31, help="Chunk size in days."),
    include_standard: bool = typer.Option(True, help="Include standard voucher base types."),
    include_custom: bool = typer.Option(True, help="Include custom or non-standard voucher base types."),
    min_count: int = typer.Option(1, help="Only sync profiled voucher types seen at least this many times."),
    continue_on_error: bool = typer.Option(False, help="Continue even if one company or voucher family fails."),
    adaptive: bool = typer.Option(True, help="Automatically split failed date windows into smaller windows."),
    min_chunk_days: int = typer.Option(1, help="Smallest window size to try when adaptive splitting is enabled."),
    sync_masters_for_each_company: bool = typer.Option(False, help="Also run master sync for every matched company before voucher sync."),
) -> None:
    init_db()
    with _tally_client() as client, get_session() as session:
        result = sync_company_family(
            session,
            client,
            selector=selector,
            chunk_days=chunk_days,
            include_standard=include_standard,
            include_custom=include_custom,
            min_count=min_count,
            continue_on_error=continue_on_error,
            adaptive=adaptive,
            min_chunk_days=min_chunk_days,
            sync_masters_for_each_company=sync_masters_for_each_company,
        )
    typer.echo(json.dumps(result, indent=2))


@command("sync-all")
def sync_all(
    company: str = typer.Option(..., help="Exact Tally company name, including FY suffix where applicable."),
    continue_on_error: bool = typer.Option(False, help="Continue even if one voucher family fails."),
) -> None:
    init_db()
    with _tally_client() as client, get_session() as session:
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


@command("report")
def report() -> None:
    init_db()
    with get_session() as session:
        result = get_database_report(session)
    typer.echo(json.dumps(result, indent=2))


@command("support-bundle")
def support_bundle(
    output_directory: str = typer.Option("./support-bundles", help="Directory where the support bundle should be created."),
    include_payload_bodies: bool = typer.Option(False, help="Include full request and response XML in the bundle."),
    redact_payload_bodies: bool = typer.Option(True, help="Redact common sensitive values when including payload bodies."),
    payload_limit: int = typer.Option(5, help="Number of recent payloads to include."),
) -> None:
    init_db()
    with get_session() as session:
        result = create_support_bundle(
            session,
            output_directory=output_directory,
            include_payload_bodies=include_payload_bodies,
            redact_payload_bodies=redact_payload_bodies,
            payload_limit=payload_limit,
        )
    typer.echo(json.dumps(result, indent=2))


@command("prune-payloads")
def prune_payloads(
    keep_latest: int = typer.Option(100, help="How many recent payloads to keep."),
    request_type: Optional[str] = typer.Option(default=None, help="Optional request type filter."),
    dry_run: bool = typer.Option(False, help="Show what would be deleted without deleting anything."),
) -> None:
    init_db()
    with get_session() as session:
        result = prune_raw_payloads(
            session,
            keep_latest=keep_latest,
            request_type=request_type,
            dry_run=dry_run,
        )
    typer.echo(json.dumps(result, indent=2))


@command("prune-legacy-global-masters")
def prune_legacy_global_masters(
    dry_run: bool = typer.Option(False, help="Show how many legacy global master rows would be deleted without deleting them."),
) -> None:
    init_db()
    with get_session() as session:
        result = prune_legacy_global_master_rows(session, dry_run=dry_run)
    typer.echo(json.dumps(result, indent=2))


@command("replay-xml")
def replay_xml(
    kind: str = typer.Option(
        ...,
        help="One of: masters, stock-groups, stock-items, units, godowns, cost-centres, stock-item-balances, voucher-types, vouchers",
    ),
    file: str = typer.Option(..., help="Path to a saved Tally XML export."),
    company: Optional[str] = typer.Option(default=None, help="Required for voucher replay."),
) -> None:
    init_db()
    with get_session() as session:
        result = replay_xml_file(session, kind=kind, file_path=file, company_name=company)
    typer.echo(json.dumps(result, indent=2))


@command("replay-bundle")
def replay_bundle(
    directory: str = typer.Option(..., help="Directory containing saved Tally XML exports."),
    company: str = typer.Option(..., help="Exact company name for voucher replay within the bundle."),
) -> None:
    init_db()
    with get_session() as session:
        result = replay_xml_bundle(session, directory=directory, company_name=company)
    typer.echo(json.dumps(result, indent=2))
