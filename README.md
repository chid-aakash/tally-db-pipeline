# tally-db-pipeline

Standalone local-first pipeline for pulling data from TallyPrime and storing it in a normal SQL database.

This repo does not depend on Sena, Frappe, or any other internal platform code. It is intended to be cloned and run directly by the customer on their own machine.

The current design is:

`CLI on your machine -> Tally HTTP/XML API -> local database`

There is no separate bridge service in this repo.

The extraction model is intentionally sequential:

- send one XML request
- wait for one XML response
- store the raw payloads and normalized rows
- then move to the next request

This is deliberate because Tally is fragile under overlapping requests.

## What this repo does

- connects directly to TallyPrime over its HTTP/XML interface
- reads master data and voucher data
- stores raw XML request/response payloads for audit/debug
- stores normalized records in a SQL database
- lets the customer use that database for reporting, exports, ETL, or custom tools

## What this repo stores

- companies
- groups
- ledgers
- stock groups
- stock items
- units
- godowns
- cost centres
- voucher types
- vouchers
- voucher inventory lines
- voucher ledger lines
- unknown/custom voucher XML sections that were not normalized yet
- raw XML payloads for every sync request
- sync run history
- sync checkpoints for incremental voucher pulls

## Important assumptions

- TallyPrime is running.
- Tally's HTTP server is enabled.
- The machine running this repo can reach the Tally machine over the network.
- Tally is usually reachable on port `9000`, but that should be confirmed in Tally settings.
- Voucher sync requires the exact Tally company name, including financial-year suffixes when present.
- For best reliability, keep the target company open inside Tally's UI while syncing.

## Before you start

You need these things:

1. Python `3.11+`
2. Git
3. Access to the machine where Tally is running
4. The Tally machine's IP address
5. The Tally HTTP port

## Step 1: Find the Tally machine IP address

If Tally is running on Windows:

1. Open `Command Prompt`
2. Run:

```bat
ipconfig
```

3. Find the active adapter
4. Copy the `IPv4 Address`

Example:

```text
IPv4 Address. . . . . . . . . . . : 10.211.55.3
```

In that example:

- `TALLY_HOST=10.211.55.3`

## Step 2: Find the Tally HTTP port

In TallyPrime, check the HTTP/XML connectivity settings.

Most installations use:

- `TALLY_PORT=9000`

If Tally is configured with another port, use that value instead.

## Step 3: Clone the repo

Open a terminal on the machine where you want to run the pipeline.

Run:

```bash
git clone <YOUR_GIT_URL_HERE> tally-db-pipeline
cd tally-db-pipeline
```

If you already downloaded the repo as a zip, just extract it and `cd` into the folder.

## Step 4: Create a Python virtual environment

On macOS or Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

On Windows PowerShell:

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
```

On Windows Command Prompt:

```bat
py -m venv .venv
.venv\Scripts\activate.bat
```

After activation, your shell should show that `.venv` is active.

## Step 5: Install the project

With the virtual environment active, run:

```bash
python -m pip install --upgrade pip
python -m pip install -e .
```

This installs the CLI command:

```bash
tally-db-pipeline
```

## Step 6: Create the `.env` file

Copy the sample environment file:

On macOS or Linux:

```bash
cp .env.example .env
```

On Windows Command Prompt:

```bat
copy .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

## Step 7: Edit the `.env` file

Open `.env` in a text editor and set the Tally host and port.

Example:

```env
TALLY_HOST=10.211.55.3
TALLY_PORT=9000
DATABASE_URL=sqlite:///./data/tally_pipeline.sqlite3
TALLY_TIMEOUT_SECONDS=120
TALLY_REQUEST_DELAY_MS=250
TALLY_MAX_RETRIES=2
TALLY_RETRY_BACKOFF_MS=1500
```

Field meanings:

- `TALLY_HOST`: IP address or hostname of the machine running Tally
- `TALLY_PORT`: Tally HTTP/XML port
- `DATABASE_URL`: where the local database should live
- `TALLY_TIMEOUT_SECONDS`: HTTP timeout for large Tally requests
- `TALLY_REQUEST_DELAY_MS`: pause between requests so Tally is not hammered
- `TALLY_MAX_RETRIES`: retry count for connection/timeouts
- `TALLY_RETRY_BACKOFF_MS`: backoff between retries

## Step 8: Confirm the machine can reach Tally

Optional but recommended.

On macOS or Linux:

```bash
nc -vz 10.211.55.3 9000
```

If successful, you should see something like:

```text
Connection to 10.211.55.3 port 9000 succeeded
```

If this fails:

- Tally may not be running
- the Tally HTTP port may not be enabled
- the IP or port may be wrong
- firewall/network rules may be blocking access

## Step 9: Check that the CLI works

Run:

```bash
tally-db-pipeline --help
```

You should see the list of available commands.

## Step 10: Test the live Tally connection

Run:

```bash
tally-db-pipeline ping
```

Expected successful output looks like:

```text
Connected to Tally at http://10.211.55.3:9000
Companies visible: 1
- Example Company - 2025-26
```

If this fails with a connection error:

- recheck `TALLY_HOST`
- recheck `TALLY_PORT`
- make sure Tally is open
- make sure the Tally HTTP server is enabled

## Step 10A: Run a fast discovery probe

Run:

```bash
tally-db-pipeline discover
```

This returns compact JSON showing:

- whether Tally is reachable
- which companies are visible
- warnings if Tally looks unhealthy or no companies are exposed

If you already know the exact company name, you can probe voucher-type access too:

```bash
tally-db-pipeline discover --company "Shanke Pvt Ltd - 2025-26"
```

## Step 10B: Run a human-readable diagnostic check

Run:

```bash
tally-db-pipeline doctor --company "Shanke Pvt Ltd - 2025-26"
```

Use this when the customer says something vague like "it worked once but not now".

`doctor` now distinguishes:

- `connection_error`
- `timeout`
- `line_error`
- empty company or master-data responses

and prints request durations so you can tell the difference between "cannot reach Tally" and "Tally is reachable but not responding usefully".

It also reports a high-level `health_status` such as:

- `healthy`
- `reachable_but_stalled`
- `reachable_but_no_companies`
- `reachable_but_no_master_data`
- `reachable_but_rejected`
- `unreachable`

## Step 11: List the available company names

Run:

```bash
tally-db-pipeline list-companies
```

This command is important because voucher sync requires the exact company name from Tally.

Example:

```text
Shanke Pvt Ltd - 2025-26
```

Copy that value exactly.

## Step 12: Initialize the database

Run:

```bash
tally-db-pipeline init-db
```

This creates the local SQLite database file automatically at:

```text
./data/tally_pipeline.sqlite3
```

You do not need to manually create the `data/` folder. The code creates it automatically for SQLite.

## Step 12A: Inspect the local database report

Run:

```bash
tally-db-pipeline report
```

This prints a JSON report with:

- local table counts
- recent sync runs
- recent failures
- whether any syncs are still marked as running

## Step 13: Pull master data

Before running this step, make sure the target company is open in Tally.

This command now uses collection-based pulls for groups and ledgers instead of starting with one heavy exploded `List of Accounts` report. That makes it much more reliable on larger Tally datasets.

Run:

```bash
tally-db-pipeline sync-masters
```

This pulls master data such as:

- ledgers
- stock items
- stock groups
- units
- godowns
- cost centres

## Step 14: Pull voucher types

Run:

```bash
tally-db-pipeline sync-voucher-types --company "Shanke Pvt Ltd - 2025-26"
```

Replace the company name with the exact value returned by `list-companies`.

This step helps the pipeline understand custom Tally voucher names such as:

- `61 Sales`
- `12 Payt Bank`
- `2 Receipts`

and map them back to base types such as:

- `Sales`
- `Payment`
- `Receipt`

## Step 15: Test one voucher family first

Before doing a full sync, test one voucher type.

Example:

```bash
tally-db-pipeline sync-vouchers --company "Shanke Pvt Ltd - 2025-26" --voucher-type Sales
```

Other common voucher types:

- `Purchase`
- `Receipt`
- `Payment`
- `Journal`
- `Contra`
- `Credit Note`
- `Debit Note`

Or run the standard accounting voucher families in one command:

```bash
tally-db-pipeline sync-standard-vouchers --company "Shanke Pvt Ltd - 2025-26"
```

If you want it to keep going even if one voucher family fails:

```bash
tally-db-pipeline sync-standard-vouchers --company "Shanke Pvt Ltd - 2025-26" --continue-on-error
```

If the customer has a large history, start with a bounded date range:

```bash
tally-db-pipeline sync-vouchers --company "Shanke Pvt Ltd - 2025-26" --voucher-type Sales --from-date 2025-04-01 --to-date 2025-04-30
```

For larger history loads, use chunked date windows:

```bash
tally-db-pipeline sync-vouchers-chunked --company "Shanke Pvt Ltd - 2025-26" --voucher-type Sales --from-date 2025-04-01 --to-date 2026-03-31 --chunk-days 31
```

After the first range-based load, use incremental sync:

```bash
tally-db-pipeline sync-vouchers-incremental --company "Shanke Pvt Ltd - 2025-26" --voucher-type Sales --since-date 2025-04-01
```

Once a checkpoint exists for that company and voucher family, later runs can omit `--since-date`.

If no checkpoint exists yet and the company name ends with a fiscal-year suffix like `- 2025-26`, incremental sync can infer the starting date as `2025-04-01`.

If the customer does not know which voucher families are actually used, profile a date range first:

```bash
tally-db-pipeline profile-vouchers --company "Shanke Pvt Ltd - 2025-26" --from-date 2025-04-01 --to-date 2026-03-31
```

For larger profiling ranges, use the chunked profiler:

```bash
tally-db-pipeline profile-vouchers-chunked --company "Shanke Pvt Ltd - 2025-26" --from-date 2025-04-01 --to-date 2026-03-31 --chunk-days 31
```

## Step 16: Run the standard full sync

Before running this step, make sure the target company is open in Tally.

Once the single voucher test works, run:

```bash
tally-db-pipeline sync-all --company "Shanke Pvt Ltd - 2025-26"
```

If you want it to keep going even if one voucher family fails:

```bash
tally-db-pipeline sync-all --company "Shanke Pvt Ltd - 2025-26" --continue-on-error
```

This runs, in order:

1. master data sync
2. voucher type sync
3. Sales vouchers
4. Purchase vouchers
5. Receipt vouchers
6. Payment vouchers
7. Journal vouchers
8. Contra vouchers
9. Credit Note vouchers
10. Debit Note vouchers

## What database gets created

By default, this repo uses SQLite:

```text
./data/tally_pipeline.sqlite3
```

That is a normal SQLite file and can be opened by:

- `sqlite3`
- DB Browser for SQLite
- DBeaver
- TablePlus
- Python scripts
- custom ETL tools

## Using Postgres instead of SQLite

If the customer wants Postgres, set `DATABASE_URL` in `.env`.

Example:

```env
DATABASE_URL=postgresql+psycopg://user:password@localhost:5432/tally_pipeline
```

Then install a Postgres driver:

```bash
python -m pip install "psycopg[binary]"
```

After that, run:

```bash
tally-db-pipeline init-db
```

## Commands reference

Show help:

```bash
tally-db-pipeline --help
```

Initialize database:

```bash
tally-db-pipeline init-db
```

Test connectivity:

```bash
tally-db-pipeline ping
```

Fast discovery JSON:

```bash
tally-db-pipeline discover
```

Human-readable diagnostics:

```bash
tally-db-pipeline doctor --company "Exact Company Name"
```

Bootstrap plan for a new customer environment:

```bash
tally-db-pipeline bootstrap
```

List companies:

```bash
tally-db-pipeline list-companies
```

Sync company metadata:

```bash
tally-db-pipeline sync-companies
```

Sync master data:

```bash
tally-db-pipeline sync-masters
```

Sync voucher types:

```bash
tally-db-pipeline sync-voucher-types --company "Exact Company Name"
```

Sync one voucher family:

```bash
tally-db-pipeline sync-vouchers --company "Exact Company Name" --voucher-type Sales
```

Sync one voucher family for a date range:

```bash
tally-db-pipeline sync-vouchers --company "Exact Company Name" --voucher-type Sales --from-date 2025-04-01 --to-date 2025-04-30
```

Sync one voucher family in deterministic chunks:

```bash
tally-db-pipeline sync-vouchers-chunked --company "Exact Company Name" --voucher-type Sales --from-date 2025-04-01 --to-date 2026-03-31 --chunk-days 31
```

Continue incrementally from checkpoints:

```bash
tally-db-pipeline sync-vouchers-incremental --company "Exact Company Name" --voucher-type Sales --since-date 2025-04-01
```

Profile voucher families in a date range:

```bash
tally-db-pipeline profile-vouchers --company "Exact Company Name" --from-date 2025-04-01 --to-date 2026-03-31
```

Profile voucher families with chunked windows:

```bash
tally-db-pipeline profile-vouchers-chunked --company "Exact Company Name" --from-date 2025-04-01 --to-date 2026-03-31 --chunk-days 31
```

Sync the standard accounting voucher families:

```bash
tally-db-pipeline sync-standard-vouchers --company "Exact Company Name"
```

Run the common end-to-end sync:

```bash
tally-db-pipeline sync-all --company "Exact Company Name"
```

Inspect the local database and recent run history:

```bash
tally-db-pipeline report
```

Create a support bundle for troubleshooting:

```bash
tally-db-pipeline support-bundle
```

Include recent raw XML bodies too:

```bash
tally-db-pipeline support-bundle --include-payload-bodies --payload-limit 3
```

Include payload bodies with basic redaction for common sensitive fields:

```bash
tally-db-pipeline support-bundle --include-payload-bodies --redact-payload-bodies --payload-limit 3
```

Preview raw payload cleanup without deleting anything:

```bash
tally-db-pipeline prune-payloads --keep-latest 100 --dry-run
```

Delete older raw payloads while keeping the latest 100:

```bash
tally-db-pipeline prune-payloads --keep-latest 100
```

Replay a saved XML export into the local database:

```bash
tally-db-pipeline replay-xml --kind voucher-types --file /path/to/voucher-types.xml
```

Replay a saved voucher export:

```bash
tally-db-pipeline replay-xml --kind vouchers --file /path/to/day-book.xml --company "Exact Company Name"
```

## Common problems

### Problem: `Cannot connect to http://<host>:<port>`

Check:

- Tally is open
- HTTP/XML is enabled in Tally
- host is correct
- port is correct
- the machine running this repo can reach the Tally machine

Then run:

```bash
tally-db-pipeline discover
```

### Problem: `list-companies` works but vouchers do not sync

Most likely cause:

- the company name passed to `--company` is not an exact match

Fix:

- run `tally-db-pipeline list-companies`
- copy the company name exactly
- use that exact string in `--company`
- run `tally-db-pipeline doctor --company "Exact Company Name"`

### Problem: customer has custom voucher names

That is normal.

Tally often uses custom names like:

- `61 Sales`
- `12 Receipts Bank`
- `2 Payt Cash`

This repo handles that by syncing voucher types first and resolving each custom type back to a base type.

### Problem: some Tally setups have more voucher families than the default sync

That is also normal.

The default `sync-all` focuses on common accounting voucher families. If the customer depends on other families like:

- `Stock Journal`
- `Sales Order`
- `Purchase Order`
- `Delivery Note`
- `Receipt Note`

then those should be tested and added explicitly.

### Problem: Tally returns strange data or empty sections

Different customers may have:

- different voucher structures
- financial-year split companies
- local customizations
- custom voucher naming conventions

The protocol is the same, but the business data shape can vary. That is why the recommended first run is always:

1. `ping`
2. `list-companies`
3. `sync-masters`
4. `sync-voucher-types`
5. `sync-vouchers` for one family
6. then `sync-all`

### Problem: `sync-masters` returns no data or says no master data was returned

Most likely cause:

- the company is not currently open in Tally's UI

Fix:

- open the target company in Tally
- stay on that company
- run `tally-db-pipeline sync-masters` again

### Problem: one run worked, later runs hang or time out

Most likely causes:

- Tally is being hit with overlapping requests
- Tally is open but busy, half-loaded, or in a different UI state
- a large voucher family is taking too long for the current timeout

Fix:

- run only one command at a time against that Tally instance
- wait for one command to finish before starting another
- run `tally-db-pipeline doctor --company "Exact Company Name"`
- inspect `tally-db-pipeline report`
- if needed, increase `TALLY_TIMEOUT_SECONDS`
- prefer `sync-vouchers-chunked` over one large historical pull
- keep adaptive chunk splitting enabled unless you are debugging a specific window manually

### Problem: company names contain `&` or other XML-sensitive characters

Fix:

- use the exact company name returned by `list-companies`
- do not manually replace `&` with escape sequences
- the CLI now escapes XML-sensitive values before sending them to Tally

### Problem: live Tally is unstable, but you have saved XML exports

You can still test parsing and database loading offline using:

```bash
tally-db-pipeline replay-xml --kind masters --file /path/to/list-of-accounts.xml
tally-db-pipeline replay-xml --kind voucher-types --file /path/to/voucher-types.xml
tally-db-pipeline replay-xml --kind vouchers --file /path/to/day-book.xml --company "Exact Company Name"
```

## Notes for customers

- Do not run multiple large syncs in parallel against the same Tally instance.
- Tally's API is sensitive and behaves best when requests are sent one at a time.
- This repo is an extraction pipeline, not a replacement for Tally.
- Always keep Tally as the source of truth unless the customer intentionally builds a downstream workflow on top of the synced database.

## Additional docs in this repo

- `TECHNICAL_RESOURCES.md`
  - official Tally references and the extraction assumptions derived from them
- `PRODUCTION_GAP_LIST.md`
  - adversarial backlog of remaining production risks
- `RELIABILITY_PLAN.md`
  - execution plan for hardening this repo further

For repeatable offline regression against the saved XML bundle:

```bash
source .venv/bin/activate
python scripts/check_replay_counts.py
```

When troubleshooting a customer environment, prefer sending a support bundle instead of screenshots:

```bash
tally-db-pipeline support-bundle
```

## Repo layout

```text
src/tally_db_pipeline/
  cli.py
  config.py
  db.py
  models.py
  parsers.py
  sync.py
  tally_client.py
```
