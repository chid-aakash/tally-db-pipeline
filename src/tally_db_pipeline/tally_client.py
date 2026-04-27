from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from pathlib import Path
from datetime import date, datetime, timedelta
from xml.sax.saxutils import escape

import requests


logger = logging.getLogger(__name__)
_INVALID_XML_CHARS = re.compile(r"&#(?:[0-8]|1[0-1]|1[4-9]|2[0-9]|3[01]);")
_MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

_VOUCHER_FILTERS = {
    "Sales": "$$IsSales:$VoucherTypeName",
    "Purchase": "$$IsPurchase:$VoucherTypeName",
    "Receipt": "$$IsReceipt:$VoucherTypeName",
    "Payment": "$$IsPayment:$VoucherTypeName",
    "Journal": "$$IsJournal:$VoucherTypeName",
    "Contra": "$$IsContra:$VoucherTypeName",
    "Credit Note": "$$IsCreditNote:$VoucherTypeName",
    "Debit Note": "$$IsDebitNote:$VoucherTypeName",
    "Sales Order": '$VoucherTypeName = "Sales Order"',
    "Purchase Order": '$VoucherTypeName = "Purchase Order"',
    "Delivery Note": '$VoucherTypeName = "Delivery Note"',
    "Receipt Note": '$VoucherTypeName = "Receipt Note"',
    "Stock Journal": '$VoucherTypeName = "Stock Journal"',
}


def _voucher_filter_formula(voucher_type: str) -> str:
    return _VOUCHER_FILTERS.get(voucher_type, f'$VoucherTypeName = "{_tdl_string(voucher_type)}"')


def _voucher_childof_expression(voucher_type: str) -> str:
    built_in = {
        "Sales": "$$VchTypeSales",
        "Purchase": "$$VchTypePurchase",
        "Receipt": "$$VchTypeReceipt",
        "Payment": "$$VchTypePayment",
        "Journal": "$$VchTypeJournal",
        "Contra": "$$VchTypeContra",
        "Credit Note": "$$VchTypeCreditNote",
        "Debit Note": "$$VchTypeDebitNote",
    }
    return built_in.get(voucher_type, f'"{_tdl_string(voucher_type)}"')


class TallyClient:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9000,
        timeout: int = 120,
        request_delay_ms: int = 250,
        max_retries: int = 2,
        retry_backoff_ms: int = 1500,
        lock_file: str = "./data/tally_http.lock",
        lock_stale_seconds: int = 21600,
    ):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.request_delay_ms = request_delay_ms
        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms
        self.lock_file = Path(lock_file)
        self.lock_stale_seconds = lock_stale_seconds
        self.base_url = f"http://{host}:{port}"
        self._last_request_started_at = 0.0
        self._lock_fd: int | None = None

    def __enter__(self) -> "TallyClient":
        self._acquire_lock()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        self._release_lock()

    def post_json(self, payload: dict, *, headers: dict[str, str]) -> str:
        last_error: Exception | None = None
        http_headers = {"content-type": "application/json", **headers}
        for attempt in range(self.max_retries + 1):
            self._wait_before_next_request()
            try:
                response = requests.post(
                    self.base_url,
                    data=json.dumps(payload).encode("utf-8"),
                    headers=http_headers,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                return response.text
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    raise
                time.sleep((self.retry_backoff_ms / 1000.0) * (attempt + 1))
        raise RuntimeError(str(last_error) if last_error else "Unknown Tally request error")

    def post(self, xml_payload: str) -> str:
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            self._wait_before_next_request()
            try:
                response = requests.post(
                    self.base_url,
                    data=xml_payload.encode("utf-8"),
                    headers={"Content-Type": "application/xml"},
                    timeout=self.timeout,
                )
                response.raise_for_status()
                text = response.text
                text = _INVALID_XML_CHARS.sub("", text)
                text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
                return text
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    raise
                time.sleep((self.retry_backoff_ms / 1000.0) * (attempt + 1))
        raise RuntimeError(str(last_error) if last_error else "Unknown Tally request error")

    def _acquire_lock(self) -> None:
        if self._lock_fd is not None:
            return
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        while True:
            try:
                fd = os.open(self.lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                payload = f"{os.getpid()} {int(time.time())}\n"
                os.write(fd, payload.encode("utf-8"))
                self._lock_fd = fd
                return
            except FileExistsError:
                try:
                    mtime = self.lock_file.stat().st_mtime
                except FileNotFoundError:
                    continue
                age = time.time() - mtime
                if age > self.lock_stale_seconds:
                    try:
                        self.lock_file.unlink()
                    except FileNotFoundError:
                        pass
                    continue
                raise RuntimeError(
                    f"Tally lock is already held by another local command: {self.lock_file}. "
                    "Wait for the other sync/probe to finish or delete a stale lock if you are sure no command is running."
                )

    def _release_lock(self) -> None:
        if self._lock_fd is None:
            return
        try:
            os.close(self._lock_fd)
        finally:
            self._lock_fd = None
            try:
                self.lock_file.unlink()
            except FileNotFoundError:
                pass

    def probe(self, request_type: str, request_xml: str) -> dict:
        started = time.monotonic()
        try:
            response_xml = self.post(request_xml)
            duration_ms = int((time.monotonic() - started) * 1000)
            line_error = self.extract_line_error(response_xml)
            return {
                "ok": line_error is None,
                "request_type": request_type,
                "request_xml": request_xml,
                "response_xml": response_xml,
                "response_sha256": hashlib.sha256(response_xml.encode("utf-8")).hexdigest(),
                "line_error": line_error,
                "error_kind": "line_error" if line_error else None,
                "error": line_error,
                "duration_ms": duration_ms,
            }
        except requests.Timeout as exc:
            duration_ms = int((time.monotonic() - started) * 1000)
            return {
                "ok": False,
                "request_type": request_type,
                "request_xml": request_xml,
                "response_xml": "",
                "response_sha256": None,
                "line_error": None,
                "error_kind": "timeout",
                "error": str(exc),
                "duration_ms": duration_ms,
            }
        except requests.ConnectionError as exc:
            duration_ms = int((time.monotonic() - started) * 1000)
            return {
                "ok": False,
                "request_type": request_type,
                "request_xml": request_xml,
                "response_xml": "",
                "response_sha256": None,
                "line_error": None,
                "error_kind": "connection_error",
                "error": str(exc),
                "duration_ms": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic() - started) * 1000)
            return {
                "ok": False,
                "request_type": request_type,
                "request_xml": request_xml,
                "response_xml": "",
                "response_sha256": None,
                "line_error": None,
                "error_kind": "unexpected_error",
                "error": str(exc),
                "duration_ms": duration_ms,
            }

    def _wait_before_next_request(self) -> None:
        now = time.monotonic()
        if self._last_request_started_at:
            min_spacing = self.request_delay_ms / 1000.0
            elapsed = now - self._last_request_started_at
            if elapsed < min_spacing:
                time.sleep(min_spacing - elapsed)
        self._last_request_started_at = time.monotonic()

    def test_connection(self) -> dict:
        try:
            request_xml = self.build_company_collection_xml()
            response_xml = self.post(request_xml)
            return {
                "connected": True,
                "request_xml": request_xml,
                "response_xml": response_xml,
                "response_sha256": hashlib.sha256(response_xml.encode("utf-8")).hexdigest(),
            }
        except requests.ConnectionError:
            return {"connected": False, "error": f"Cannot connect to {self.base_url}"}
        except Exception as exc:
            return {"connected": False, "error": str(exc)}

    @staticmethod
    def build_company_collection_xml() -> str:
        return (
            "<ENVELOPE>"
            "<HEADER>"
            "<VERSION>1</VERSION>"
            "<TALLYREQUEST>EXPORT</TALLYREQUEST>"
            "<TYPE>COLLECTION</TYPE>"
            "<ID>CompanyInfo</ID>"
            "</HEADER>"
            "<BODY><DESC>"
            "<STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>"
            "<TDL><TDLMESSAGE>"
            '<COLLECTION NAME="CompanyInfo" ISINITIALIZE="Yes">'
            "<TYPE>Company</TYPE>"
            "<NATIVEMETHOD>Name</NATIVEMETHOD>"
            "<NATIVEMETHOD>FormalName</NATIVEMETHOD>"
            "<NATIVEMETHOD>BasicCurrencyCode</NATIVEMETHOD>"
            "<NATIVEMETHOD>Country</NATIVEMETHOD>"
            "<NATIVEMETHOD>StateName</NATIVEMETHOD>"
            "<NATIVEMETHOD>PINCode</NATIVEMETHOD>"
            "<NATIVEMETHOD>Phone</NATIVEMETHOD>"
            "<NATIVEMETHOD>Email</NATIVEMETHOD>"
            "<NATIVEMETHOD>GSTN</NATIVEMETHOD>"
            "<NATIVEMETHOD>IncomeTaxNumber</NATIVEMETHOD>"
            "</COLLECTION>"
            "</TDLMESSAGE></TDL>"
            "</DESC></BODY>"
            "</ENVELOPE>"
        )

    @staticmethod
    def build_report_xml(report_name: str, explode: bool = True, company: str | None = None) -> str:
        explode_flag = "<EXPLODEFLAG>Yes</EXPLODEFLAG>" if explode else ""
        company_xml = f"<SVCURRENTCOMPANY>{_xml(company)}</SVCURRENTCOMPANY>" if company else ""
        return (
            "<ENVELOPE>"
            "<HEADER>"
            "<VERSION>1</VERSION>"
            "<TALLYREQUEST>Export</TALLYREQUEST>"
            "<TYPE>Data</TYPE>"
            f"<ID>{_xml(report_name)}</ID>"
            "</HEADER>"
            "<BODY><DESC><STATICVARIABLES>"
            f"{explode_flag}{company_xml}<SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>"
            "</STATICVARIABLES></DESC></BODY>"
            "</ENVELOPE>"
        )

    @staticmethod
    def build_collection_xml(name: str, object_type: str, fields: list[str] | None = None, company: str | None = None) -> str:
        methods = "".join(f"<NATIVEMETHOD>{_xml(field)}</NATIVEMETHOD>" for field in fields) if fields else "<NATIVEMETHOD>*</NATIVEMETHOD>"
        company_xml = f"<SVCURRENTCOMPANY>{_xml(company)}</SVCURRENTCOMPANY>" if company else ""
        return (
            "<ENVELOPE>"
            "<HEADER>"
            "<VERSION>1</VERSION>"
            "<TALLYREQUEST>EXPORT</TALLYREQUEST>"
            "<TYPE>COLLECTION</TYPE>"
            f"<ID>{_xml(name)}</ID>"
            "</HEADER>"
            "<BODY><DESC>"
            f"<STATICVARIABLES>{company_xml}<SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>"
            "<TDL><TDLMESSAGE>"
            f'<COLLECTION NAME="{_xml_attr(name)}" ISINITIALIZE="Yes">'
            f"<TYPE>{_xml(object_type)}</TYPE>"
            f"{methods}"
            "</COLLECTION>"
            "</TDLMESSAGE></TDL>"
            "</DESC></BODY>"
            "</ENVELOPE>"
        )

    @staticmethod
    def build_voucher_collection_xml(company: str, voucher_type: str) -> str:
        filter_name = re.sub(r"[^A-Za-z0-9]", "", voucher_type) + "Filter"
        filter_formula = _voucher_filter_formula(voucher_type)
        return (
            "<ENVELOPE>"
            "<HEADER>"
            "<VERSION>1</VERSION>"
            "<TALLYREQUEST>EXPORT</TALLYREQUEST>"
            "<TYPE>COLLECTION</TYPE>"
            "<ID>AllVouchers</ID>"
            "</HEADER>"
            "<BODY><DESC>"
            "<STATICVARIABLES>"
            f"<SVCURRENTCOMPANY>{_xml(company)}</SVCURRENTCOMPANY>"
            "<SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>"
            "</STATICVARIABLES>"
            "<TDL><TDLMESSAGE>"
            '<COLLECTION NAME="AllVouchers" ISINITIALIZE="Yes">'
            "<TYPE>Voucher</TYPE>"
            f"<FILTER>{_xml(filter_name)}</FILTER>"
            "<FETCH>*, ALLLEDGERENTRIES, ALLINVENTORYENTRIES</FETCH>"
            "</COLLECTION>"
            f'<SYSTEM TYPE="Formulae" NAME="{_xml_attr(filter_name)}">{_xml(filter_formula)}</SYSTEM>'
            "</TDLMESSAGE></TDL>"
            "</DESC></BODY>"
            "</ENVELOPE>"
        )

    @staticmethod
    def _date_range_filter_formula(from_date: str | None, to_date: str | None) -> str | None:
        # NOTE: Tally's "<=" comparison against $$Date:"YYYYMMDD" is unreliable
        # for certain dates (reproducibly fails for Feb 28 among others and
        # drops the entire result set). Use a half-open interval instead:
        # [from_date, to_date + 1 day).
        parts: list[str] = []
        if from_date:
            parts.append(f'$Date &gt;= $$Date:"{_format_tally_report_date(from_date)}"')
        if to_date:
            exclusive_end = _coerce_date(to_date) + timedelta(days=1)
            parts.append(f'$Date &lt; $$Date:"{exclusive_end.strftime("%Y%m%d")}"')
        return " and ".join(parts) if parts else None

    @staticmethod
    def build_voucher_type_collection_range_xml(
        company: str,
        voucher_type: str,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        full_fetch: bool = True,
    ) -> str:
        if from_date and not to_date:
            to_date = from_date
        if to_date and not from_date:
            from_date = to_date

        static_variables = [
            f"<SVCURRENTCOMPANY>{_xml(company)}</SVCURRENTCOMPANY>",
            "<SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>",
        ]
        if from_date:
            static_variables.append(f'<SVFROMDATE TYPE="Date">{_xml(_format_tally_report_date(from_date))}</SVFROMDATE>')
        if to_date:
            static_variables.append(f'<SVTODATE TYPE="Date">{_xml(_format_tally_report_date(to_date))}</SVTODATE>')

        collection_name = "RangeVouchers"
        type_filter_name = "VchTypeFilter"
        date_filter_name = "VchDateFilter"
        type_formula = _voucher_filter_formula(voucher_type)
        date_formula = TallyClient._date_range_filter_formula(from_date, to_date)

        filter_refs = [type_filter_name]
        system_formulas = [(type_filter_name, type_formula)]
        if date_formula:
            filter_refs.append(date_filter_name)
            system_formulas.append((date_filter_name, date_formula))

        fetch = "*, ALLLEDGERENTRIES.*, ALLINVENTORYENTRIES.*" if full_fetch else "Date, VoucherNumber, PartyLedgerName, PartyName, VoucherTypeName, GUID"
        systems_xml = "".join(
            f'<SYSTEM TYPE="Formulae" NAME="{_xml_attr(n)}">{f}</SYSTEM>' for n, f in system_formulas
        )
        return (
            "<ENVELOPE>"
            "<HEADER>"
            "<VERSION>1</VERSION>"
            "<TALLYREQUEST>EXPORT</TALLYREQUEST>"
            "<TYPE>COLLECTION</TYPE>"
            f"<ID>{collection_name}</ID>"
            "</HEADER>"
            "<BODY><DESC>"
            f"<STATICVARIABLES>{''.join(static_variables)}</STATICVARIABLES>"
            "<TDL><TDLMESSAGE>"
            f'<COLLECTION NAME="{collection_name}" ISINITIALIZE="Yes">'
            "<TYPE>Voucher</TYPE>"
            f"<FILTER>{','.join(filter_refs)}</FILTER>"
            f"<FETCH>{fetch}</FETCH>"
            "</COLLECTION>"
            f"{systems_xml}"
            "</TDLMESSAGE></TDL>"
            "</DESC></BODY>"
            "</ENVELOPE>"
        )

    @staticmethod
    def build_voucher_collection_range_xml(
        company: str,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        full_fetch: bool = False,
    ) -> str:
        if from_date and not to_date:
            to_date = from_date
        if to_date and not from_date:
            from_date = to_date

        static_variables = [
            f"<SVCURRENTCOMPANY>{_xml(company)}</SVCURRENTCOMPANY>",
            "<SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>",
        ]
        if from_date:
            static_variables.append(f'<SVFROMDATE TYPE="Date">{_xml(_format_tally_report_date(from_date))}</SVFROMDATE>')
        if to_date:
            static_variables.append(f'<SVTODATE TYPE="Date">{_xml(_format_tally_report_date(to_date))}</SVTODATE>')

        collection_name = "RangeAllVouchers"
        date_filter_name = "AllVchDateFilter"
        date_formula = TallyClient._date_range_filter_formula(from_date, to_date)
        filter_xml = f"<FILTER>{date_filter_name}</FILTER>" if date_formula else ""
        system_xml = (
            f'<SYSTEM TYPE="Formulae" NAME="{_xml_attr(date_filter_name)}">{date_formula}</SYSTEM>'
            if date_formula
            else ""
        )

        fetch = "*, ALLLEDGERENTRIES.*, ALLINVENTORYENTRIES.*" if full_fetch else "Date, VoucherNumber, PartyLedgerName, PartyName, VoucherTypeName, GUID"
        return (
            "<ENVELOPE>"
            "<HEADER>"
            "<VERSION>1</VERSION>"
            "<TALLYREQUEST>EXPORT</TALLYREQUEST>"
            "<TYPE>COLLECTION</TYPE>"
            f"<ID>{collection_name}</ID>"
            "</HEADER>"
            "<BODY><DESC>"
            f"<STATICVARIABLES>{''.join(static_variables)}</STATICVARIABLES>"
            "<TDL><TDLMESSAGE>"
            f'<COLLECTION NAME="{collection_name}" ISINITIALIZE="Yes">'
            "<TYPE>Voucher</TYPE>"
            f"{filter_xml}"
            f"<FETCH>{fetch}</FETCH>"
            "</COLLECTION>"
            f"{system_xml}"
            "</TDLMESSAGE></TDL>"
            "</DESC></BODY>"
            "</ENVELOPE>"
        )

    @staticmethod
    def build_voucher_alterid_collection_xml(
        company: str,
        *,
        since_alter_id: int,
        upto_alter_id: int | None = None,
        full_fetch: bool = True,
        from_date: str = "2000-01-01",
        to_date: str = "2099-12-31",
    ) -> str:
        # Incremental sync: returns every voucher with $AlterID > since_alter_id.
        # A wide SVFROMDATE/SVTODATE is required — Tally's Voucher collection is
        # period-scoped, and without explicit dates it silently restricts to an
        # empty window (a single-digit result). The AlterID filter itself is
        # date-agnostic; the period just defines the visible universe.
        collection_name = "AlterIdVouchers"
        filter_name = "AlterIdGtFilter"
        formula = f"$AlterID &gt; {int(since_alter_id)}"
        if upto_alter_id is not None:
            formula = f"({formula}) AND ($AlterID &lt;= {int(upto_alter_id)})"
        fetch = "*, ALLLEDGERENTRIES.*, ALLINVENTORYENTRIES.*" if full_fetch else "Date, VoucherNumber, PartyLedgerName, PartyName, VoucherTypeName, GUID, AlterID, MasterID"
        return (
            "<ENVELOPE>"
            "<HEADER>"
            "<VERSION>1</VERSION>"
            "<TALLYREQUEST>EXPORT</TALLYREQUEST>"
            "<TYPE>COLLECTION</TYPE>"
            f"<ID>{collection_name}</ID>"
            "</HEADER>"
            "<BODY><DESC>"
            "<STATICVARIABLES>"
            f"<SVCURRENTCOMPANY>{_xml(company)}</SVCURRENTCOMPANY>"
            "<SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>"
            f'<SVFROMDATE TYPE="Date">{_xml(_format_tally_report_date(from_date))}</SVFROMDATE>'
            f'<SVTODATE TYPE="Date">{_xml(_format_tally_report_date(to_date))}</SVTODATE>'
            "</STATICVARIABLES>"
            "<TDL><TDLMESSAGE>"
            f'<COLLECTION NAME="{collection_name}" ISINITIALIZE="Yes">'
            "<TYPE>Voucher</TYPE>"
            f"<FILTER>{filter_name}</FILTER>"
            f"<FETCH>{fetch}</FETCH>"
            "</COLLECTION>"
            f'<SYSTEM TYPE="Formulae" NAME="{_xml_attr(filter_name)}">{formula}</SYSTEM>'
            "</TDLMESSAGE></TDL>"
            "</DESC></BODY>"
            "</ENVELOPE>"
        )

    @staticmethod
    def build_daybook_xml(
        company: str,
        voucher_type: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> str:
        if from_date and not to_date:
            to_date = from_date
        if to_date and not from_date:
            from_date = to_date

        static_variables = [
            f"<SVCURRENTCOMPANY>{_xml(company)}</SVCURRENTCOMPANY>",
            "<SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>",
        ]
        if from_date:
            static_variables.append(f'<SVFROMDATE TYPE="Date">{_xml(_format_tally_report_date(from_date))}</SVFROMDATE>')
        if to_date:
            static_variables.append(f'<SVTODATE TYPE="Date">{_xml(_format_tally_report_date(to_date))}</SVTODATE>')

        tdl = ""
        if voucher_type:
            filter_name = re.sub(r"[^A-Za-z0-9]", "", voucher_type) + "Filter"
            filter_formula = _voucher_filter_formula(voucher_type)
            tdl = (
                "<TDL><TDLMESSAGE>"
                '<REPORT NAME="Day Book" ISMODIFY="Yes" ISFIXED="No" ISINITIALIZE="No" ISOPTION="No" ISINTERNAL="No">'
                f"<LOCAL>Collection : Default : Add :Filter : {_xml(filter_name)}</LOCAL>"
                "<LOCAL>Collection : Default : Add :Fetch : VoucherTypeName</LOCAL>"
                "</REPORT>"
                f'<SYSTEM TYPE="Formulae" NAME="{_xml_attr(filter_name)}">{_xml(filter_formula)}</SYSTEM>'
                "</TDLMESSAGE></TDL>"
            )

        return (
            "<ENVELOPE>"
            "<HEADER>"
            "<VERSION>1</VERSION>"
            "<TALLYREQUEST>Export</TALLYREQUEST>"
            "<TYPE>Data</TYPE>"
            "<ID>DayBook</ID>"
            "</HEADER>"
            "<BODY><DESC>"
            f"<STATICVARIABLES>{''.join(static_variables)}</STATICVARIABLES>"
            f"{tdl}"
            "</DESC></BODY>"
            "</ENVELOPE>"
        )

    @staticmethod
    def build_voucher_import_xml(company: str, voucher: dict) -> str:
        voucher_type = voucher["voucher_type"]
        action = voucher.get("action", "Create")
        objview = voucher.get("objview", "Accounting Voucher View")
        date_str = _format_tally_report_date(voucher["date"])
        effective_date_str = _format_tally_report_date(voucher.get("effective_date") or voucher["date"])

        header_fields = []
        header_fields.append(f"<DATE>{_xml(date_str)}</DATE>")
        header_fields.append(f"<EFFECTIVEDATE>{_xml(effective_date_str)}</EFFECTIVEDATE>")
        header_fields.append(f"<VOUCHERTYPENAME>{_xml(voucher_type)}</VOUCHERTYPENAME>")
        if voucher.get("voucher_number"):
            header_fields.append(f"<VOUCHERNUMBER>{_xml(str(voucher['voucher_number']))}</VOUCHERNUMBER>")
        if voucher.get("reference"):
            header_fields.append(f"<REFERENCE>{_xml(str(voucher['reference']))}</REFERENCE>")
        if voucher.get("party_ledger_name"):
            header_fields.append(f"<PARTYLEDGERNAME>{_xml(voucher['party_ledger_name'])}</PARTYLEDGERNAME>")
            header_fields.append(f"<PARTYNAME>{_xml(voucher['party_ledger_name'])}</PARTYNAME>")
        if voucher.get("narration"):
            header_fields.append(f"<NARRATION>{_xml(voucher['narration'])}</NARRATION>")
        if voucher.get("remote_id"):
            header_fields.append(f"<REMOTEID>{_xml(voucher['remote_id'])}</REMOTEID>")
        if voucher.get("is_optional"):
            header_fields.append("<ISOPTIONAL>Yes</ISOPTIONAL>")

        ledger_entries_xml = "".join(
            TallyClient._build_ledger_entry_xml(entry) for entry in voucher.get("ledger_entries") or []
        )
        inventory_entries = voucher.get("inventory_entries") or []
        # Stock journals use INVENTORYENTRIESIN.LIST/OUT.LIST; detect by presence of a 'direction' field.
        if inventory_entries and any(e.get("direction") in ("in", "out") for e in inventory_entries):
            inventory_entries_xml = "".join(
                TallyClient._build_stock_journal_entry_xml(e) for e in inventory_entries
            )
        else:
            inventory_entries_xml = "".join(
                TallyClient._build_inventory_entry_xml(entry) for entry in inventory_entries
            )

        voucher_xml = (
            f'<VOUCHER VCHTYPE="{_xml_attr(voucher_type)}" ACTION="{_xml_attr(action)}" OBJVIEW="{_xml_attr(objview)}">'
            + "".join(header_fields)
            + ledger_entries_xml
            + inventory_entries_xml
            + "</VOUCHER>"
        )

        return (
            "<ENVELOPE>"
            "<HEADER>"
            "<VERSION>1</VERSION>"
            "<TALLYREQUEST>Import</TALLYREQUEST>"
            "<TYPE>Data</TYPE>"
            "<ID>Vouchers</ID>"
            "</HEADER>"
            "<BODY>"
            "<DESC><STATICVARIABLES>"
            f"<SVCURRENTCOMPANY>{_xml(company)}</SVCURRENTCOMPANY>"
            "</STATICVARIABLES></DESC>"
            "<DATA>"
            '<TALLYMESSAGE xmlns:UDF="TallyUDF">'
            f"{voucher_xml}"
            "</TALLYMESSAGE>"
            "</DATA>"
            "</BODY>"
            "</ENVELOPE>"
        )

    @staticmethod
    def _build_ledger_entry_xml(entry: dict) -> str:
        # Tally sign convention: ISDEEMEDPOSITIVE=Yes emits a negative AMOUNT (debit-style);
        # No emits positive. Callers pass a signed amount and the flag; we write both verbatim.
        amount = entry["amount"]
        is_deemed_positive = bool(entry.get("is_deemed_positive", False))
        parts = [
            f"<LEDGERNAME>{_xml(entry['ledger_name'])}</LEDGERNAME>",
            f"<ISDEEMEDPOSITIVE>{'Yes' if is_deemed_positive else 'No'}</ISDEEMEDPOSITIVE>",
            f"<LEDGERFROMITEM>No</LEDGERFROMITEM>",
            f"<REMOVEZEROENTRIES>No</REMOVEZEROENTRIES>",
            f"<ISPARTYLEDGER>{'Yes' if entry.get('is_party_ledger') else 'No'}</ISPARTYLEDGER>",
            f"<AMOUNT>{_format_amount(amount)}</AMOUNT>",
        ]
        for bill in entry.get("bill_allocations") or []:
            parts.append(TallyClient._build_bill_allocation_xml(bill))
        return "<ALLLEDGERENTRIES.LIST>" + "".join(parts) + "</ALLLEDGERENTRIES.LIST>"

    @staticmethod
    def _build_bill_allocation_xml(bill: dict) -> str:
        return (
            "<BILLALLOCATIONS.LIST>"
            f"<NAME>{_xml(bill['name'])}</NAME>"
            f"<BILLTYPE>{_xml(bill.get('bill_type', 'New Ref'))}</BILLTYPE>"
            f"<AMOUNT>{_format_amount(bill['amount'])}</AMOUNT>"
            "</BILLALLOCATIONS.LIST>"
        )

    @staticmethod
    def _build_inventory_entry_xml(entry: dict) -> str:
        parts = [
            f"<STOCKITEMNAME>{_xml(entry['item_name'])}</STOCKITEMNAME>",
            f"<ISDEEMEDPOSITIVE>{'Yes' if entry.get('is_deemed_positive') else 'No'}</ISDEEMEDPOSITIVE>",
            f"<RATE>{entry.get('rate', 0)}</RATE>",
            f"<AMOUNT>{_format_amount(entry['amount'])}</AMOUNT>",
            f"<ACTUALQTY>{entry['quantity']} {entry.get('uom', '')}</ACTUALQTY>",
            f"<BILLEDQTY>{entry['quantity']} {entry.get('uom', '')}</BILLEDQTY>",
        ]
        if entry.get("godown"):
            parts.append(
                "<BATCHALLOCATIONS.LIST>"
                f"<GODOWNNAME>{_xml(entry['godown'])}</GODOWNNAME>"
                f"<AMOUNT>{_format_amount(entry['amount'])}</AMOUNT>"
                f"<ACTUALQTY>{entry['quantity']} {entry.get('uom', '')}</ACTUALQTY>"
                f"<BILLEDQTY>{entry['quantity']} {entry.get('uom', '')}</BILLEDQTY>"
                "</BATCHALLOCATIONS.LIST>"
            )
        if entry.get("accounting_ledger"):
            parts.append(
                "<ACCOUNTINGALLOCATIONS.LIST>"
                f"<LEDGERNAME>{_xml(entry['accounting_ledger'])}</LEDGERNAME>"
                f"<ISDEEMEDPOSITIVE>{'Yes' if entry.get('accounting_is_deemed_positive') else 'No'}</ISDEEMEDPOSITIVE>"
                f"<AMOUNT>{_format_amount(entry['amount'])}</AMOUNT>"
                "</ACCOUNTINGALLOCATIONS.LIST>"
            )
        return "<ALLINVENTORYENTRIES.LIST>" + "".join(parts) + "</ALLINVENTORYENTRIES.LIST>"

    @staticmethod
    def _build_stock_journal_entry_xml(entry: dict) -> str:
        # Stock journal tag semantics (verified against live Tally exports):
        #   direction="out" -> INVENTORYENTRIESOUT.LIST = source / consumed side (shown on left in UI),
        #                       AMOUNT positive, ISDEEMEDPOSITIVE=No.
        #   direction="in"  -> INVENTORYENTRIESIN.LIST  = destination / produced side (shown on right),
        #                       AMOUNT negative, ISDEEMEDPOSITIVE=Yes, godown goes here.
        # Quantities stay positive on both sides; only AMOUNT carries the sign.
        direction = entry["direction"]
        tag = "INVENTORYENTRIESOUT.LIST" if direction == "out" else "INVENTORYENTRIESIN.LIST"
        uom = entry.get("uom", "No.")
        qty = abs(float(entry["quantity"]))
        qty_str = f" {qty:g} {uom}"
        amount = entry.get("amount")  # Optional — omit tag when absent/zero
        is_deemed_positive = entry.get("is_deemed_positive")
        if is_deemed_positive is None:
            is_deemed_positive = direction == "in"
        # Enforce sign convention so callers can pass positive amounts: IN
        # entries render correctly only when AMOUNT is negative (Tally
        # otherwise prefixes the value with "(-)" in the destination column).
        if amount is not None and direction == "in":
            amount = -abs(float(amount))
        rate = entry.get("rate")
        rate_str = f"{float(rate):.2f}/{uom}" if rate is not None and float(rate) != 0 else ""

        parts = [
            f"<STOCKITEMNAME>{_xml(entry['item_name'])}</STOCKITEMNAME>",
            f"<ISDEEMEDPOSITIVE>{'Yes' if is_deemed_positive else 'No'}</ISDEEMEDPOSITIVE>",
        ]
        if rate_str:
            parts.append(f"<RATE>{_xml(rate_str)}</RATE>")
        if amount is not None:
            parts.append(f"<AMOUNT>{_format_amount(amount)}</AMOUNT>")
        parts.extend([
            f"<ACTUALQTY>{qty_str}</ACTUALQTY>",
            f"<BILLEDQTY>{qty_str}</BILLEDQTY>",
        ])
        description = entry.get("description")
        if description:
            parts.append(
                "<BASICUSERDESCRIPTION.LIST>"
                f"<BASICUSERDESCRIPTION>{_xml(description)}</BASICUSERDESCRIPTION>"
                "</BASICUSERDESCRIPTION.LIST>"
            )

        batch_parts = [f"<BATCHNAME>{_xml(entry.get('batch', 'Primary Batch'))}</BATCHNAME>"]
        if entry.get("godown"):
            batch_parts.insert(0, f"<GODOWNNAME>{_xml(entry['godown'])}</GODOWNNAME>")
        if amount is not None:
            batch_parts.append(f"<AMOUNT>{_format_amount(amount)}</AMOUNT>")
        batch_parts.extend([
            f"<ACTUALQTY>{qty_str}</ACTUALQTY>",
            f"<BILLEDQTY>{qty_str}</BILLEDQTY>",
        ])
        if rate_str:
            batch_parts.append(f"<BATCHRATE>{_xml(rate_str)}</BATCHRATE>")
        parts.append("<BATCHALLOCATIONS.LIST>" + "".join(batch_parts) + "</BATCHALLOCATIONS.LIST>")

        return f"<{tag}>" + "".join(parts) + f"</{tag}>"

    @staticmethod
    def parse_import_response(response_xml: str) -> dict:
        def _int_tag(name: str) -> int | None:
            m = re.search(rf"<{name}>(-?\d+)</{name}>", response_xml)
            return int(m.group(1)) if m else None

        def _str_tag(name: str) -> str | None:
            m = re.search(rf"<{name}>(.*?)</{name}>", response_xml, re.DOTALL)
            return m.group(1).strip() if m else None

        line_error = TallyClient.extract_line_error(response_xml)
        created = _int_tag("CREATED") or 0
        altered = _int_tag("ALTERED") or 0
        ignored = _int_tag("IGNORED") or 0
        errors = _int_tag("ERRORS") or 0
        return {
            "ok": line_error is None and errors == 0 and (created + altered) > 0,
            "created": created,
            "altered": altered,
            "ignored": ignored,
            "errors": errors,
            "last_vch_id": _int_tag("LASTVCHID"),
            "last_master_id": _int_tag("LASTMID"),
            "line_error": line_error,
            "exception": _str_tag("EXCEPTIONS") or _str_tag("DESC"),
        }

    def fetch_voucher_number_by_master_id(self, company: str, master_id: int | str) -> str | None:
        """Look up a voucher's human-readable VoucherNumber by its Tally Master ID (LASTVCHID)."""
        xml = (
            "<ENVELOPE><HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST>"
            "<TYPE>Collection</TYPE><ID>VchByMasterId</ID></HEADER><BODY><DESC>"
            f"<STATICVARIABLES><SVCURRENTCOMPANY>{_xml(company)}</SVCURRENTCOMPANY></STATICVARIABLES>"
            "<TDL><TDLMESSAGE>"
            "<COLLECTION NAME=\"VchByMasterId\" ISMODIFY=\"No\">"
            "<TYPE>Voucher</TYPE>"
            f"<FILTER>IsTargetVch</FILTER></COLLECTION>"
            f"<SYSTEM TYPE=\"Formulae\" NAME=\"IsTargetVch\">$MasterId = {int(master_id)}</SYSTEM>"
            "</TDLMESSAGE></TDL></DESC></BODY></ENVELOPE>"
        )
        try:
            resp = self.post(xml)
        except Exception:
            return None
        m = re.search(r"<VOUCHERNUMBER>(.*?)</VOUCHERNUMBER>", resp, re.DOTALL)
        return m.group(1).strip() if m and m.group(1).strip() else None

    def import_voucher(self, company: str, voucher: dict, *, dry_run: bool = False) -> dict:
        request_xml = self.build_voucher_import_xml(company, voucher)
        if dry_run:
            return {"dry_run": True, "request_xml": request_xml}
        response_xml = self.post(request_xml)
        result = self.parse_import_response(response_xml)
        result["request_xml"] = request_xml
        result["response_xml"] = response_xml
        result["response_sha256"] = hashlib.sha256(response_xml.encode("utf-8")).hexdigest()
        return result

    @staticmethod
    def build_voucher_import_json(company: str, voucher: dict) -> dict:
        vtype = voucher["voucher_type"]
        action = voucher.get("action", "Create")
        objview = voucher.get("objview", "Accounting Voucher View")
        date_str = _format_tally_report_date(voucher["date"])
        effective_date_str = _format_tally_report_date(voucher.get("effective_date") or voucher["date"])

        message: dict = {
            "metadata": {
                "type": "Voucher",
                "vchtype": vtype,
                "action": action,
                "objview": objview,
            },
            "date": date_str,
            "effectivedate": effective_date_str,
            "vouchertypename": vtype,
        }
        if voucher.get("voucher_number"):
            message["vouchernumber"] = str(voucher["voucher_number"])
        if voucher.get("reference"):
            message["reference"] = str(voucher["reference"])
        if voucher.get("party_ledger_name"):
            message["partyledgername"] = voucher["party_ledger_name"]
            message["partyname"] = voucher["party_ledger_name"]
        if voucher.get("narration"):
            message["narration"] = voucher["narration"]
        if voucher.get("remote_id"):
            message["remoteid"] = voucher["remote_id"]
        if voucher.get("is_optional"):
            message["isoptional"] = True
        if voucher.get("is_invoice"):
            message["isinvoice"] = True

        ledger_entries = voucher.get("ledger_entries") or []
        if ledger_entries:
            message["ledgerentries"] = [TallyClient._build_ledger_entry_json(e) for e in ledger_entries]

        inventory_entries = voucher.get("inventory_entries") or []
        if inventory_entries:
            message["allinventoryentries"] = [TallyClient._build_inventory_entry_json(e) for e in inventory_entries]

        return {
            "static_variables": [
                {"name": "svVchImportFormat", "value": "jsonex"},
                {"name": "svCurrentCompany", "value": company},
            ],
            "tallymessage": [message],
        }

    @staticmethod
    def _build_ledger_entry_json(entry: dict) -> dict:
        out: dict = {
            "ledgername": entry["ledger_name"],
            "isdeemedpositive": bool(entry.get("is_deemed_positive", False)),
            "ispartyledger": bool(entry.get("is_party_ledger", False)),
            "ledgerfromitem": False,
            "removezeroentries": False,
            "amount": _format_amount(entry["amount"]),
        }
        bill_allocations = entry.get("bill_allocations") or []
        if bill_allocations:
            out["billallocations"] = [
                {
                    "name": b["name"],
                    "billtype": b.get("bill_type", "New Ref"),
                    "amount": _format_amount(b["amount"]),
                }
                for b in bill_allocations
            ]
        return out

    @staticmethod
    def _build_inventory_entry_json(entry: dict) -> dict:
        qty_str = f" {entry['quantity']} {entry.get('uom', '')}".rstrip()
        out: dict = {
            "stockitemname": entry["item_name"],
            "isdeemedpositive": bool(entry.get("is_deemed_positive", False)),
            "rate": f"{entry.get('rate', 0)}/{entry.get('uom', 'nos')}",
            "amount": _format_amount(entry["amount"]),
            "actualqty": qty_str,
            "billedqty": qty_str,
        }
        if entry.get("godown"):
            out["batchallocations"] = [
                {
                    "godownname": entry["godown"],
                    "batchname": entry.get("batch", "Primary Batch"),
                    "destinationgodownname": entry["godown"],
                    "amount": _format_amount(entry["amount"]),
                    "actualqty": qty_str,
                    "billedqty": qty_str,
                }
            ]
        if entry.get("accounting_ledger"):
            out["accountingallocations"] = [
                {
                    "ledgername": entry["accounting_ledger"],
                    "isdeemedpositive": bool(entry.get("accounting_is_deemed_positive", False)),
                    "ledgerfromitem": False,
                    "removezeroentries": False,
                    "ispartyledger": False,
                    "amount": _format_amount(entry["amount"]),
                }
            ]
        return out

    @staticmethod
    def parse_import_response_json(response_text: str) -> dict:
        try:
            payload = json.loads(response_text)
        except json.JSONDecodeError:
            return {
                "ok": False,
                "parse_error": True,
                "raw_response": response_text,
            }
        result = (payload.get("data") or {}).get("import_result") or {}
        status = str(payload.get("status", "")).strip()
        created = int(result.get("created", 0) or 0)
        altered = int(result.get("altered", 0) or 0)
        errors = int(result.get("errors", 0) or 0)
        exceptions = int(result.get("exceptions", 0) or 0)
        return {
            "ok": status == "1" and errors == 0 and exceptions == 0 and (created + altered) > 0,
            "status": status,
            "created": created,
            "altered": altered,
            "deleted": int(result.get("deleted", 0) or 0),
            "ignored": int(result.get("ignored", 0) or 0),
            "errors": errors,
            "exceptions": exceptions,
            "cancelled": int(result.get("cancelled", 0) or 0),
            "combined": int(result.get("combined", 0) or 0),
            "last_vch_id": result.get("lastvchid"),
            "last_master_id": result.get("lastmid"),
            "vch_number": result.get("vchnumber"),
            "raw_response": payload,
        }

    def import_voucher_json(self, company: str, voucher: dict, *, dry_run: bool = False) -> dict:
        payload = self.build_voucher_import_json(company, voucher)
        if dry_run:
            return {"dry_run": True, "request_payload": payload}
        response_text = self.post_json(
            payload,
            headers={
                "version": "1",
                "tallyrequest": "Import",
                "type": "Data",
                "id": "Vouchers",
            },
        )
        result = self.parse_import_response_json(response_text)
        result["request_payload"] = payload
        result["response_text"] = response_text
        return result

    def execute(self, request_type: str, request_xml: str) -> dict:
        response_xml = self.post(request_xml)
        return {
            "request_type": request_type,
            "request_xml": request_xml,
            "response_xml": response_xml,
            "response_sha256": hashlib.sha256(response_xml.encode("utf-8")).hexdigest(),
        }

    @staticmethod
    def extract_line_error(response_xml: str) -> str | None:
        match = re.search(r"<LINEERROR>(.*?)</LINEERROR>", response_xml, re.DOTALL)
        if not match:
            return None
        return match.group(1).replace("&apos;", "'").strip()


def _xml(value: str) -> str:
    return escape(value, {'"': "&quot;"})


def _xml_attr(value: str) -> str:
    return escape(value, {'"': "&quot;", "'": "&apos;"})


def _tdl_string(value: str) -> str:
    return value.replace('"', '\\"')


def _format_tally_date(raw: str) -> str:
    parsed = _coerce_date(raw)
    return f"{parsed.day}-{_MONTH_ABBR[parsed.month - 1]}-{parsed.year}"


def _format_tally_report_date(raw: str) -> str:
    parsed = _coerce_date(raw)
    return parsed.strftime("%Y%m%d")


def _format_amount(amount: float | int | str) -> str:
    if isinstance(amount, str):
        return amount
    return f"{float(amount):.4f}"


def _coerce_date(raw: str) -> date:
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%d-%b-%Y", "%d-%B-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported date format: {raw}. Use YYYY-MM-DD.")
