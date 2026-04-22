from __future__ import annotations

import hashlib
import logging
import os
import re
import time
from pathlib import Path
from datetime import date, datetime
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
        if voucher_type not in _VOUCHER_FILTERS:
            raise ValueError(f"Unsupported voucher type: {voucher_type}")
        filter_name = re.sub(r"[^A-Za-z0-9]", "", voucher_type) + "Filter"
        filter_formula = _VOUCHER_FILTERS[voucher_type]
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
            static_variables.append(f'<SVFROMDATE TYPE="Date">{_xml(_format_tally_date(from_date))}</SVFROMDATE>')
        if to_date:
            static_variables.append(f'<SVTODATE TYPE="Date">{_xml(_format_tally_date(to_date))}</SVTODATE>')

        tdl = ""
        if voucher_type:
            if voucher_type not in _VOUCHER_FILTERS:
                raise ValueError(f"Unsupported voucher type: {voucher_type}")
            filter_name = re.sub(r"[^A-Za-z0-9]", "", voucher_type) + "Filter"
            filter_formula = _VOUCHER_FILTERS[voucher_type]
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


def _format_tally_date(raw: str) -> str:
    parsed = _coerce_date(raw)
    return f"{parsed.day}-{_MONTH_ABBR[parsed.month - 1]}-{parsed.year}"


def _coerce_date(raw: str) -> date:
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%d-%b-%Y", "%d-%B-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported date format: {raw}. Use YYYY-MM-DD.")
