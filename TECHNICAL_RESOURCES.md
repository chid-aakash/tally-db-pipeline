# Technical Resources

Curated reference set for understanding TallyPrime integration behavior.

These are the documents to consult before changing extraction logic, request formats, or assumptions about how Tally behaves.

## Primary official references

- Tally Developer Reference
  - https://help.tallysolutions.com/developer-reference/
  - Why it matters: top-level portal for TDL, integration, troubleshooting, and developer tooling.

- Integration With TallyPrime
  - https://help.tallysolutions.com/integration-with-tallyprime/
  - Why it matters: core XML/HTTP integration overview.
  - Key takeaways:
    - TallyPrime can act as an HTTP server for XML requests.
    - The port is configurable, commonly `9000`.
    - A company must be loaded in TallyPrime.
    - Error responses can appear inside `<LINEERROR>`.

- Integration Using XML Interface
  - https://help.tallysolutions.com/xml-interface/
  - Why it matters: concise overview of XML-based integration capabilities.

- Getting Started with Tally Integrations
  - https://help.tallysolutions.com/getting-started-with-tally-integrations/
  - Why it matters: high-level integration model and supported protocols.

- XML Integration
  - https://help.tallysolutions.com/xml-integration/
  - Why it matters: standard message structure and request semantics.

- Sample XML
  - https://help.tallysolutions.com/sample-xml/
  - Why it matters: concrete request/response examples for object, collection, report, and voucher extraction.
  - Key takeaways:
    - Tally supports `Object`, `Collection`, and `Data` export request types.
    - `DayBook` supports `SVFROMDATE` / `SVTODATE` and TDL-based voucher-type filtering.
    - Error information can be returned in `<LINEERROR>` even when HTTP succeeds.

- TallyPrime as a Server
  - https://help.tallysolutions.com/developer-reference/integration-using-xml-interface/tallyprime-as-server/
  - Why it matters: confirms the XML-over-HTTP request model and the basic POST pattern.

- Collection Level Attributes for Integration
  - https://help.tallysolutions.com/how-to-use-collection-level-attributes-for-integration/
  - Why it matters: key TDL collection mechanics including remote URL, HTTP XML, request/response mapping, and XML object paths.

- TDL Reference Manual
  - https://help.tallysolutions.com/seriesa/rel-5-4/en/help/TDL_Reference_Manual.pdf
  - Why it matters: deep reference for collections, filters, object types, and TDL semantics.

- TDL FAQ
  - https://help.tallysolutions.com/developer-reference/developer-reference-faq/tdl-faq/
  - Why it matters: useful for edge cases around dates, variables like `SVFromDate` and `SVToDate`, and collection patterns.

- JSON Integration
  - https://help.tallysolutions.com/tally-prime-integration-using-json-1/
  - Why it matters: TallyPrime 7.0 introduces native JSON as a first-class path.
  - Why this matters for the repo:
    - XML remains the most proven current path for us.
    - JSON is a future candidate for improved extraction reliability on supported installs.

- Tally Connector
  - https://help.tallysolutions.com/developer-reference/tally-prime-developer-tools/tally-connector/
  - Why it matters: official tooling for experimenting with XML/JSON requests and responses.

- TallyPrime API Explorer
  - https://tallysolutions.com/tallyprime-api-explorer/
  - Why it matters: interactive environment for testing XML/JSON requests outside the repo.

## Secondary references

- Accounting-Companion/TallyConnector
  - https://github.com/Accounting-Companion/TallyConnector
  - Why it matters: useful external implementation reference for request construction, field scoping, and practical Tally integration ergonomics.
  - Caveat: not the source of truth; official Tally docs win when there is disagreement.

## Repo-specific conclusions from the docs

- Tally is stateful, not a clean stateless API server.
- The active/open company matters for some data access paths.
- XML error signaling is often embedded in a successful HTTP response body.
- Collections and reports behave differently; one working does not imply the other will work.
- Day Book is the most promising deterministic path for chunked voucher extraction because the official examples support both date windows and voucher-type filtering.
- Request shape should be kept deterministic and narrow.
- Diagnostics must distinguish:
  - network reachability
  - company visibility
  - report availability
  - collection availability
  - timeout/stall conditions

## Working assumptions to revisit later

- Whether native JSON in TallyPrime 7.0 is more stable than XML for large extracts.
- Whether voucher extraction can be chunked more safely with TDL filters over date ranges.
- Whether more deterministic report paths exist for master data than `List of Accounts`.
