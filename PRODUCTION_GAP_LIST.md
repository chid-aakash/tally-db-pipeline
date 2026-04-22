# Production Gap List

Working list of features, risks, and hardening work needed to make `tally-db-pipeline` customer-grade for varied Tally installations.

This file is intentionally adversarial. Items stay here until the repo can either handle them deterministically or fail clearly with operator guidance.

See also [LIVE_VALIDATION_NOTES.md](LIVE_VALIDATION_NOTES.md) for the running truth source from real Tally environments.

## Current findings from live testing

- Tally can accept TCP connections and still return no visible companies.
- Tally can return different answers for the same high-level workflow across runs.
- Master-data sync depends on the target company being open in Tally's UI.
- Voucher sync can work once and later hang or time out.
- Overlapping requests are risky; Tally behaves like a fragile single-threaded server.
- Large report/data exports are more failure-prone than small collection probes.
- Some Tally instances appear to ignore Day Book date windows even when `SVFROMDATE` / `SVTODATE` are supplied.

## Test assets available locally

- Live Tally VM reachable from the Mac test machine.
- Saved XML exports under `office/_workspace-admin/data-dumps/tally-xml-exports/`
  - `list-of-accounts.xml`
  - `voucher-types.xml`
  - `day-book.xml`
  - `stock-items.xml`
  - `stock-groups.xml`
  - `godowns.xml`
  - `units.xml`
  - `cost-centres.xml`
  - `stock-summary.xml`
  - `trial-balance.xml`
- Raw Tally data archive:
  - `office/_workspace-admin/data-dumps/Tally Data-03.04.26.zip`

## Critical

- Incremental voucher sync with checkpoints.
  - Current state: checkpoint table exists and incremental voucher command exists.
  - Remaining gaps:
    - checkpointing is date-based, not object-level
    - initial backfill still needs an explicit start date
    - partial-window retries need stronger resume semantics

- Chunked voucher extraction.
  - Current state: date-window chunking exists for voucher pulls, and the repo now refuses to accept a chunk response if voucher dates fall outside the requested window.
  - Remaining gaps:
    - chunk sizing starts static but now supports adaptive window splitting on failure
    - no response-size-aware tuning yet
    - collection-based range mode is now the default for dated voucher pulls after live validation on our Tally
    - still needs broader validation on other Tally datasets

- XML-safe request construction.
  - Current state: dynamic XML values are now escaped before sending requests.
  - Remaining gaps:
    - keep reviewing any newly added dynamic tags so we do not regress on special-character handling.

- Clear separation between discovery-safe requests and heavy extraction requests.
  - Current state: some probes can still be too heavy if we are careless.
  - Needed: strict fast probes for diagnostics.

- Date-range correctness for voucher exports.
  - Current state: profiled/chunked voucher commands now validate that returned voucher dates actually stay inside the requested window.
  - Remaining gaps:
    - still need stronger live evidence on when `daybook` should be retained as a useful fallback on other Tally datasets

- Accidental local concurrency.
  - Current state: Tally-facing CLI commands use a local lock file to prevent overlapping commands from the same machine.
  - Remaining gaps:
    - no cross-machine locking, which is outside the scope of this local-first repo

- Strong failure semantics for “reachable but unusable” Tally states.
  - Current state: `discover` and `doctor` now surface request durations, classify `connection_error`, `timeout`, `line_error`, and empty-data responses, and emit `health_status` plus recommended actions.
  - Remaining gaps:
    - no probe history/trend view yet

- Master-data extraction strategy.
  - Current state: `sync-masters` now uses collection-based group and ledger pulls instead of starting with the heavy `List of Accounts` exploded report.
  - Remaining gaps:
    - report-based fallback is still absent if some installs behave differently on collections
    - need broader live validation on other Tally datasets

## High

- Offline replay coverage from saved XML exports.
  - Current state: added replay commands and bundle replay, but there are no automated regression checks yet.
  - Needed: repeatable test matrix over all saved XML files.

- Voucher-family profiling.
  - Current state: `profile-vouchers` and `profile-vouchers-chunked` can inspect a date range and summarize voucher types from collection-based voucher pulls, `sync-profiled-vouchers` can use that profile to drive extraction, and company-family profiling/sync now exists for separate FY-suffixed companies.
  - Remaining gaps:
    - fiscal-year inference is still based on company-name suffixes
    - company-family workflows still depend on the same date-range extraction path, so they correctly fail if Tally does not honor date windows

- Cross-company master scoping.
  - Current state: master tables and voucher types are now company-scoped, and runtime SQLite migration rebuilds legacy tables into the new shape.
  - Remaining gaps:
    - older local DBs can still contain legacy blank-company rows from pre-migration runs until pruned
    - broader Postgres validation is still needed

- Unknown/custom structure preservation.
  - Current state: unknown/custom voucher child sections are preserved per voucher in `voucher_unknown_sections`.
  - Remaining gaps:
    - no normalization yet for those preserved sections
    - master-data custom sections are not preserved separately yet

- Better reporting around partial success.
  - Current state: we can see per-run outcomes in `sync_runs`, and the single-family CLI now exposes matched exact voucher types for normalized family requests.
  - Remaining gaps:
    - chunked/backfill commands still need clearer progress and per-window summaries
    - operator-facing reporting should clearly distinguish "family visible in profile" from "family proven at scale"

- Support/debug handoff bundles.
  - Current state: `support-bundle` exports a local report, settings snapshot, and recent payload metadata, with optional payload-body redaction.
  - Remaining gaps:
    - no log file rotation yet
    - redaction is pattern-based and not exhaustive

- Retention or dedupe policy for raw payloads.
  - Current state: operators can prune payload history with `prune-payloads`.
  - Remaining gaps:
    - no scheduled retention policy yet
    - no response-sha based dedupe yet

## Medium

- Postgres-first verification.
  - Current state: SQLite is the default and most-tested path.
  - Needed: verify all commands against Postgres.

- JSON path exploration for TallyPrime versions that support native JSON.
  - Current state: XML-only runtime path.
  - Needed: evaluate whether JSON improves reliability on supported installs.

- Adapter/plugin layer for customer-specific deterministic extensions.
  - Needed: support weird voucher families or custom extraction logic without forking the core repo.

- Safer long-running command ergonomics.
  - Needed: progress messages, elapsed time, first-chunk visibility, and better timeout suggestions.

## Lower priority

- Packaged sample datasets for local CI.
- Export bundles for support/debug handoff.
- Checksums/diffing for master-data change detection.

## Immediate next implementation targets

1. Live-validate collection-based dated voucher pulls on more Tally datasets.
2. Add replay-based regression scripts over saved XML exports.
3. Add richer discovery/profile commands for voucher families in use.
4. Tighten upgrade cleanup for legacy blank-company master rows.
