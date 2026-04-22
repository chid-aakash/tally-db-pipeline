# Reliability Plan

Working plan for turning `tally-db-pipeline` into a production-grade Tally ingestion toolkit.

This plan is intentionally aggressive. Each phase exists to remove a real failure mode observed either in official Tally documentation, live testing, or replayed XML fixtures.

## Ground truth

- Tally is not a stateless API service.
- A reachable TCP port does not guarantee a usable Tally state.
- A loaded company matters.
- XML errors can be returned inside a successful HTTP response.
- Large historical pulls are the highest-risk extraction pattern.
- Customer datasets vary more in business shape than in transport protocol.

## Phase 1: Safer transport and observability

Status:
- done: request pacing
- done: retry/backoff
- done: `discover`
- done: `doctor`
- done: `report`
- done: raw payload capture
- done: sync checkpoints
- done: XML escaping for dynamic request values

Remaining:
- classify timeouts separately from line errors and empty-data responses
- add explicit request duration metrics to sync reports
- add operator-facing warnings when Tally looks reachable but non-responsive

## Phase 2: Deterministic voucher extraction

Status:
- done: standard voucher families
- done: date-window voucher pulls
- done: chunked voucher pulls
- done: incremental voucher pulls from checkpoints
- done: adaptive chunk splitting on failed windows
- done: fiscal-year start inference from company names like `- 2025-26`

Remaining:
- add first-success bootstrap flow for customers who do not know the earliest useful date
- add safer resume semantics when one chunk fails in the middle of a larger backfill

## Phase 3: Discovery and profiling

Target outcomes:
- detect what companies are actually visible
- detect what voucher families are actually in use
- detect whether the instance is accounting-heavy or inventory-heavy
- detect when master-data reports are unavailable but collection probes still work

Concrete work:
- done: voucher-family profiler using bounded Day Book pulls
- add company-health summary with clear pass/fail signals
- add profile output suitable for attaching to support requests

## Phase 4: Parser coverage and loss prevention

Target outcomes:
- never silently drop business-significant sections
- preserve unknown/custom structures for later mapping
- support odd but valid customer data shapes without forking core logic

Concrete work:
- add raw XML fragment capture for unknown voucher sublists
- add explicit unmapped-fields tables or JSON columns
- add parser fixtures for unusual stock/manufacturing and banking cases
- review live and saved data for:
  - multiple inventory lines
  - duplicate-looking ledger lists
  - bank allocations
  - bill allocations
  - GST and rate detail oddities

## Phase 5: Regression discipline

Target outcomes:
- every parser or sync change can be checked against real saved exports
- bundle replay becomes the minimum bar before shipping

Concrete work:
- keep replay bundle as a standard regression command
- add scripted regression runner for saved XML fixtures
- expand local fixture coverage beyond the current export bundle
- add expected-count assertions for fixture bundles where counts are known

## Phase 6: Customer deployment quality

Target outcomes:
- a customer can clone, configure, run, and recover without us on a call
- failures become diagnosable in one pass

Concrete work:
- keep README brutally specific
- add troubleshooting matrices by symptom
- add operator checklist for first run, re-run, and unstable-Tally cases
- verify Postgres as a first-class target, not just SQLite

## Phase 7: Extension model

Target outcomes:
- weird customer cases do not require forking the repo
- deterministic extensions stay separate from the stable core

Concrete work:
- define plugin or adapter hooks for custom voucher families and fields
- make extension points explicit in docs
- preserve raw payload fidelity so future adapters can be built from evidence

## Current highest-value next steps

1. Add adaptive chunk retry logic for voucher backfills.
2. Add scripted replay regression checks with expected counts.
3. Add unknown-structure preservation for custom voucher sections.
4. Add fiscal-year-aware discovery and bootstrap flows.
5. Verify the whole command set against Postgres.
