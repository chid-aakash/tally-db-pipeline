# Production Gap List

Working list of features, risks, and hardening work needed to make `tally-db-pipeline` customer-grade for varied Tally installations.

This file is intentionally adversarial. Items stay here until the repo can either handle them deterministically or fail clearly with operator guidance.

## Current findings from live testing

- Tally can accept TCP connections and still return no visible companies.
- Tally can return different answers for the same high-level workflow across runs.
- Master-data sync depends on the target company being open in Tally's UI.
- Voucher sync can work once and later hang or time out.
- Overlapping requests are risky; Tally behaves like a fragile single-threaded server.
- Large report/data exports are more failure-prone than small collection probes.

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
  - Current state: date-window chunking exists for voucher pulls.
  - Remaining gaps:
    - chunk sizing starts static but now supports adaptive window splitting on failure
    - no response-size-aware tuning yet
    - no automatic company fiscal-year discovery yet

- XML-safe request construction.
  - Current state: dynamic XML values are now escaped before sending requests.
  - Remaining gaps:
    - keep reviewing any newly added dynamic tags so we do not regress on special-character handling.

- Clear separation between discovery-safe requests and heavy extraction requests.
  - Current state: some probes can still be too heavy if we are careless.
  - Needed: strict fast probes for diagnostics.

- Strong failure semantics for “reachable but unusable” Tally states.
  - Current state: better than before, but still not complete.
  - Needed: explicit operator-facing errors for no-company, no-data, stale UI context, and timeouts.

## High

- Offline replay coverage from saved XML exports.
  - Current state: added replay commands and bundle replay, but there are no automated regression checks yet.
  - Needed: repeatable test matrix over all saved XML files.

- Voucher-family profiling.
  - Current state: `profile-vouchers` can inspect a date range and summarize voucher types seen in Day Book output.
  - Remaining gaps:
    - no fiscal-year auto-discovery
    - no multi-window aggregate profiling yet for extremely large datasets

- Unknown/custom structure preservation.
  - Needed: store unmapped or custom object sections so we do not silently lose data.

- Better reporting around partial success.
  - Needed: show which families succeeded, failed, timed out, or returned zero rows.

- Retention or dedupe policy for raw payloads.
  - Current state: every payload is retained forever.

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
  - Needed: progress messages, elapsed time, and better timeout suggestions.

## Lower priority

- Packaged sample datasets for local CI.
- Export bundles for support/debug handoff.
- Checksums/diffing for master-data change detection.

## Immediate next implementation targets

1. Add sync-state/checkpoint tables for incremental voucher sync.
2. Add chunked voucher extraction strategy.
3. Add replay-based regression scripts over saved XML exports.
4. Add richer discovery/profile commands for voucher families in use.
