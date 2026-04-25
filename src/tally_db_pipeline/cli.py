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
    sync_voucher_types,
    sync_vouchers,
    sync_vouchers_by_alterid,
    sync_vouchers_in_chunks,
    sync_vouchers_incremental,
)
from .audits import export_ledger_prefix_mismatches, find_ledger_prefix_mismatches
from .tally_client import TallyClient


app = typer.Typer(no_args_is_help=True, add_completion=False)


def _make_voucher_progress_emitter(every: int = 100):
    def emit(event: dict) -> None:
        index = event.get("index") or 0
        total = event.get("total")
        if index and (index % every == 0 or (total and index == total)):
            suffix = f"/{total}" if total else ""
            vtype = event.get("voucher_type_name") or ""
            label = f" [{vtype}]" if vtype else ""
            typer.echo(f"  saved {index}{suffix}{label}")
    return emit


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
def sync_masters_command(company: Optional[str] = typer.Option(default=None, help="Exact Tally company name if multiple are loaded.")) -> None:
    init_db()
    with _tally_client() as client, get_session() as session:
        result = sync_masters(session, client, company_name=company)
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
    progress_every: int = typer.Option(100, help="Emit a progress line every N vouchers saved. 0 to disable."),
) -> None:
    init_db()
    progress_cb = _make_voucher_progress_emitter(progress_every) if progress_every > 0 else None
    with _tally_client() as client, get_session() as session:
        result = sync_vouchers(
            session,
            client,
            company_name=company,
            voucher_type=voucher_type,
            from_date=from_date,
            to_date=to_date,
            range_mode=range_mode,
            voucher_progress_callback=progress_cb,
        )
    typer.echo(f"Synced {result['saved']} vouchers for type: {result['voucher_type']}")
    matched_voucher_types = result.get("matched_voucher_types") or []
    if matched_voucher_types:
        typer.echo(f"Matched exact voucher types: {', '.join(matched_voucher_types)}")
    if result.get("from_date") or result.get("to_date"):
        typer.echo(f"Date range: {result.get('from_date') or result.get('to_date')} to {result.get('to_date') or result.get('from_date')}")


@command("sync-vouchers-by-alterid")
def sync_vouchers_by_alterid_command(
    company: str = typer.Option(..., help="Exact Tally company name, including FY suffix where applicable."),
    since_alter_id: Optional[int] = typer.Option(
        default=None,
        help="Starting AlterID threshold. Omit to resume from the last checkpoint (0 on first run).",
    ),
    progress_every: int = typer.Option(100, help="Emit a progress line every N vouchers saved. 0 to disable."),
) -> None:
    init_db()
    progress_cb = _make_voucher_progress_emitter(progress_every) if progress_every > 0 else None
    with _tally_client() as client, get_session() as session:
        result = sync_vouchers_by_alterid(
            session,
            client,
            company_name=company,
            since_alter_id=since_alter_id,
            voucher_progress_callback=progress_cb,
        )
    typer.echo(
        f"AlterID sync: since={result['since_alter_id']} fetched={result['fetched']} "
        f"saved={result['saved']} new_max_alter_id={result['max_alter_id']}"
    )
    matched = result.get("matched_voucher_types") or []
    if matched:
        typer.echo(f"Voucher types touched: {', '.join(matched)}")


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
    progress_every: int = typer.Option(100, help="Emit a progress line every N vouchers saved. 0 to disable."),
) -> None:
    init_db()
    progress_cb = _make_voucher_progress_emitter(progress_every) if progress_every > 0 else None
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
            voucher_progress_callback=progress_cb,
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
    progress_every: int = typer.Option(100, help="Emit a progress line every N vouchers saved. 0 to disable."),
) -> None:
    init_db()
    progress_cb = _make_voucher_progress_emitter(progress_every) if progress_every > 0 else None
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
            voucher_progress_callback=progress_cb,
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
    progress_every: int = typer.Option(100, help="Emit a progress line every N vouchers saved. 0 to disable."),
) -> None:
    init_db()
    progress_cb = _make_voucher_progress_emitter(progress_every) if progress_every > 0 else None
    with _tally_client() as client, get_session() as session:
        results = sync_standard_vouchers(
            session,
            client,
            company_name=company,
            continue_on_error=continue_on_error,
            voucher_progress_callback=progress_cb,
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
    progress_every: int = typer.Option(100, help="Emit a progress line every N vouchers saved. 0 to disable."),
) -> None:
    init_db()
    progress_cb = _make_voucher_progress_emitter(progress_every) if progress_every > 0 else None
    with _tally_client() as client, get_session() as session:
        sync_companies(session, client)
        sync_masters(session, client)
        sync_voucher_types(session, client, company_name=company)
        results = sync_standard_vouchers(
            session,
            client,
            company_name=company,
            continue_on_error=continue_on_error,
            voucher_progress_callback=progress_cb,
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


@command("ledger-prefix-audit")
def ledger_prefix_audit_command(
    company: Optional[str] = typer.Option(default=None, help="Restrict to one company. Omit for all."),
    output: Optional[str] = typer.Option(default=None, help="If given, write results to this CSV path."),
    summary_only: bool = typer.Option(False, help="Print only the count, not each row."),
) -> None:
    init_db()
    with get_session() as session:
        if output:
            result = export_ledger_prefix_mismatches(session, output, company_name=company)
            typer.echo(f"Wrote {result['row_count']} rows to {result['output_path']}")
            return
        rows = find_ledger_prefix_mismatches(session, company_name=company)
    typer.echo(f"Mismatches: {len(rows)}")
    if rows:
        from collections import Counter
        for co, n in Counter(r["company_name"] for r in rows).most_common():
            typer.echo(f"  {co}: {n}")
    if summary_only:
        return
    current_company: str | None = None
    for r in rows:
        if r["company_name"] != current_company:
            current_company = r["company_name"]
            typer.echo(f"--- {current_company} ---")
        typer.echo(
            f"  {r['voucher_date']} | {r['voucher_type']:24s} | "
            f"{r['voucher_number']:22s} | ledger={r['ledger_name']:40s} | amt={r['amount']}"
        )


@command("create-voucher")
def create_voucher_command(
    company: Optional[str] = typer.Option(
        default=None,
        help="Default Tally company name. Used for any voucher that doesn't specify its own 'company' field.",
    ),
    file: str = typer.Option(..., help="Path to a JSON file with a voucher spec, or a list of voucher specs."),
    dry_run: bool = typer.Option(False, help="Build and print the import request without POSTing to Tally."),
    continue_on_error: bool = typer.Option(False, help="Keep processing remaining vouchers if one fails."),
    format: str = typer.Option("xml", help="Import wire format: 'xml' (IMPORTDATA envelope) or 'json' (JSONEx, requires TallyPrime 7.0)."),
) -> None:
    if format not in {"xml", "json"}:
        typer.echo(f"Unknown --format {format!r}; must be 'xml' or 'json'.", err=True)
        raise typer.Exit(code=1)

    with open(file, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    vouchers = payload if isinstance(payload, list) else [payload]

    any_failed = False
    with _tally_client() as client:
        for idx, voucher in enumerate(vouchers, start=1):
            target_company = voucher.get("company") or company
            if not target_company:
                typer.echo(
                    f"[{idx}] FAIL: no company specified (neither voucher 'company' field nor --company)",
                    err=True,
                )
                if not continue_on_error:
                    raise typer.Exit(code=1)
                any_failed = True
                continue

            if format == "xml":
                result = client.import_voucher(target_company, voucher, dry_run=dry_run)
            else:
                result = client.import_voucher_json(target_company, voucher, dry_run=dry_run)

            if dry_run:
                typer.echo(f"[{idx}] DRY RUN format={format} company={target_company!r}:")
                if format == "xml":
                    typer.echo(result["request_xml"])
                else:
                    typer.echo(json.dumps(result["request_payload"], indent=2))
                continue

            status = "OK" if result["ok"] else "FAIL"
            typer.echo(
                f"[{idx}] {status} format={format} company={target_company!r} "
                f"created={result.get('created', 0)} altered={result.get('altered', 0)} "
                f"ignored={result.get('ignored', 0)} errors={result.get('errors', 0)} "
                f"last_vch_id={result.get('last_vch_id')}"
            )
            if result.get("line_error"):
                typer.echo(f"     line_error: {result['line_error']}")
            exc = result.get("exception") or result.get("exceptions")
            if exc and str(exc) not in ("0", "None"):
                typer.echo(f"     exception: {exc}")
            if not result["ok"]:
                any_failed = True
                if not continue_on_error:
                    raise typer.Exit(code=1)
    if any_failed:
        raise typer.Exit(code=1)


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


@command("policy-load")
def policy_load_command(
    file: str = typer.Option(..., help="Path to JSON policy file (see data/sj_policy_avinash.json)."),
) -> None:
    """Load / upsert voucher-type -> stock-group policy from a JSON file."""
    from pathlib import Path
    from .policy import load_policy_file

    init_db()
    with get_session() as session:
        summary = load_policy_file(session, Path(file))
    typer.echo(json.dumps(summary, indent=2))


@command("serve")
def serve_command(
    host: str = typer.Option("0.0.0.0", help="Host interface to bind."),
    port: int = typer.Option(8000, help="Port to listen on."),
    reload: bool = typer.Option(False, help="Enable auto-reload (development)."),
) -> None:
    """Start the production-entry web app."""
    import uvicorn

    init_db()
    uvicorn.run(
        "tally_db_pipeline.webapp.main:app",
        host=host,
        port=port,
        reload=reload,
    )
