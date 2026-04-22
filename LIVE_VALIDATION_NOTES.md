# Live Validation Notes

Running notes from real Tally environments. This file is the practical truth source for what the toolkit has actually been proven to handle, what still degrades under load, and what operating practices are currently required.

## Environment validated on 2026-04-22

- Tally host: `10.211.55.3`
- Tally port: `9000`
- Company: `Avinash Industries - Chennai Unit - 2025-26`
- Important nuance: this company name is not a hard historical cutoff. The business is still posting vouchers into April 2026.

## What is confirmed working

### Connectivity and discovery

- `ping` works against the live Tally instance when Tally is responsive.
- `list-companies` can discover the loaded company.
- `discover` and `doctor` can distinguish:
  - healthy
  - reachable but stalled
  - timeout
  - connection error

### Master data

- `sync-masters` works reliably after switching away from the heavy `List of Accounts` report path.
- Live-confirmed current extraction on this Tally:
  - groups: `284`
  - ledgers: `2969`
  - stock groups: `342`
  - stock items: `2755`
  - units: `24`
  - godowns: `112`
  - cost centres: `311`

### Voucher type discovery

- `sync-voucher-types` works live.
- Live-confirmed result on this Tally:
  - `118` voucher types synced

### Sales vouchers

- Sales extraction now works over broad historical ranges using the collection-based dated voucher path.
- Live-confirmed results:
  - `2026-04-01` to `2026-04-21`: `40` Sales vouchers
  - `2025-04-01` to `2026-04-21`: `754` Sales vouchers
- SQLite verification after the long-range sync:
  - count: `754`
  - min date: `2025-04-05`
  - max date: `2026-04-21`

### Other proven voucher families on a smaller live window

Live-confirmed for `2026-04-01` to `2026-04-21`:

- `Purchase`: `93`
- `Payment`: `132`
- `Receipt`: `45`
- `Journal`: `5`
- `Material Out`: `44`
- `Stock Journal` family: `420`
- `Purchase Order` family: `32`

These are real end-to-end syncs into SQLite, not just profile visibility.

### Exact voucher type matching versus normalized family matching

- The CLI `--voucher-type` input is best understood as a family selector, not always a single exact Tally voucher type.
- Some requests match one exact Tally type:
  - `Sales` matched `61 Sales` on this Tally
  - `Material Out` matched `Material Out`
- Some requests match multiple exact Tally types under one normalized family:
  - `Payment` matched exact types such as:
    - `32 Payt Bank`
    - `12 Payt Bank`
    - `72 Payt Bank`
    - `52 Payt Bank`
    - `12 Payt Cash`
    - `22 Payt Bank`
    - `92 Payment Bank`
  - `Stock Journal` family matched:
    - `Stock Journal`
    - `Conversion Stock Journal`
    - `Store Movement Stk Jrl`
    - `Stock Journal - 2 Spot MFG`
    - `Stock Journal - 3 PS MFG`
    - `Stock Journal - 4 MFG GP Sub Ass`
    - `Stock Journal - 1 MFG Assembly`
  - `Purchase Order` family matched:
    - `Purchase Order`
    - `Purchase Order -Others`

This is intentional behavior, and the CLI now exposes the matched exact voucher types in command output.

### Voucher family profiling

- Profiling can see a much wider universe of vouchers than we had initially synced.
- Live-confirmed `profile-vouchers --from-date 2025-04-01 --to-date 2026-04-21` total:
  - `30545` vouchers
- Large visible families include:
  - `Conversion Stock Journal`: `6881`
  - `Stock Journal`: `4827`
  - `31-Puchase(Monthly)` / base `Purchase`: `2929`
  - `11 Journal` / base `Journal`: `1158`
  - `Material Out`: `1068`
  - `32 Payt Bank` / base `Payment`: `955`
  - `12 Payt Bank` / base `Payment`: `950`
  - `61 Sales` / base `Sales`: `754`
  - `Purchase Order`: `490`
  - `Purchase Order -Others`: `432`
  - `72 Receipts Bank` / base `Receipt`: `320`
  - `62 Receipts Bank` / base `Receipt`: `284`
  - `Receipt Note`: `243`

## What was fixed during live testing

### Day Book under-fetched vouchers on this Tally

- Old behavior:
  - dated `Sales` sync for `2026-04-01` to `2026-04-21` returned only `2` vouchers
- Root cause:
  - the Day Book export path under-fetched badly on this Tally
- Fix:
  - dated voucher sync now uses collection-based extraction by default

### Large voucher payloads crashed XML parsing

- Old behavior:
  - long-range `Sales` sync failed with `unbound prefix`
- Root cause:
  - Tally returned XML with undeclared namespace-like prefixes in large payloads
- Fix:
  - prefixed tag and attribute names are flattened before XML parse

### Heavy master sync path timed out

- Old behavior:
  - master sync based on `List of Accounts` was too heavy and timed out
- Fix:
  - master sync now prefers collection-based group and ledger pulls

### Chunked historical backfills now show progress and start with recent windows

- Old behavior:
  - chunked voucher backfills started oldest-first and could sit silently on old history before landing any useful recent data
- Fix:
  - chunked and incremental voucher sync now process newest windows first by default
  - chunked CLI commands now emit per-window `START`, success, and error output

## What is visible but not yet proven reliable end-to-end

- `Purchase` over the full `2025-04-01` to `2026-04-21` range:
  - visible in profile
  - attempted live sync hung without writing rows
- `Receipt` over the full `2025-04-01` to `2026-04-21` range:
  - visible in profile
  - attempted live sync hung without writing rows
- `Payment` over the full `2025-04-01` to `2026-04-21` range:
  - visible in profile
  - attempted chunked live sync hung without writing rows

This does not mean the data is inaccessible. It means full-range extraction for these heavier families still needs more tuning on this Tally.

One concrete live failure pattern now confirmed:

- `Purchase` chunked historical backfill still fails date-range validation on this Tally even after newest-first chunking.
- Example:
  - requested chunk: `2026-03-31..2026-04-03`
  - Tally returned voucher rows from later dates such as `2026-04-06`, `2026-04-07`, `2026-04-08`, `2026-04-09`, `2026-04-10`
- Interpretation:
  - the current remaining problem is not silence or lack of progress visibility
  - it is Tally returning out-of-window rows for some narrower `Purchase` collection requests on this install

## Current interpretation of access limits

### Accessible now

- Company discovery
- Health diagnostics
- Group and ledger master sync
- Voucher type sync
- Sales voucher sync over broad historical ranges
- Voucher-family profiling over broad historical ranges

### Exposed by Tally and proven on smaller windows, but not yet production-proven for broad historical backfills

- Full-range Purchase sync
- Full-range Payment sync
- Full-range Receipt sync
- Full-range Stock Journal family sync
- Full-range Material Out sync
- Full-range Purchase Order family sync

### Not currently guaranteed

- That every large voucher family can be pulled in a single whole-year run without chunk tuning
- That Tally remains responsive after repeated heavy requests
- That profile visibility automatically implies stable end-to-end extraction for that family

## Current operating practices

- Run one Tally-facing command at a time.
- Prefer collection-based dated voucher sync.
- Treat Day Book as informational, not as the primary extraction path.
- Start with `doctor` if Tally behavior suddenly changes.
- Prefer shorter windows when validating a new family.
- Use chunking for heavy families.
- Expect chunked backfills to process newest windows first unless explicitly configured otherwise.
- If Tally begins stalling, restart Tally and return to the Gateway screen before resuming tests.

## Next validation targets

1. Prove broad historical backfill for `Purchase` using safer chunk sizing and observable progress.
2. Prove broad historical backfill for `Payment` using the same approach.
3. Prove broad historical backfill for `Receipt`.
4. Prove broad historical backfill for an inventory-heavy family such as `Stock Journal`.
5. Find a deterministic historical extraction strategy for families where Tally still leaks out-of-window rows under narrow collection requests.
