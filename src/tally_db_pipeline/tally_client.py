from __future__ import annotations

import hashlib
import logging
import re

import requests


logger = logging.getLogger(__name__)
_INVALID_XML_CHARS = re.compile(r"&#(?:[0-8]|1[0-1]|1[4-9]|2[0-9]|3[01]);")

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
    def __init__(self, host: str = "127.0.0.1", port: int = 9000, timeout: int = 120):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.base_url = f"http://{host}:{port}"

    def post(self, xml_payload: str) -> str:
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
    def build_report_xml(report_name: str, explode: bool = True) -> str:
        explode_flag = "<EXPLODEFLAG>Yes</EXPLODEFLAG>" if explode else ""
        return (
            "<ENVELOPE>"
            "<HEADER>"
            "<VERSION>1</VERSION>"
            "<TALLYREQUEST>Export</TALLYREQUEST>"
            "<TYPE>Data</TYPE>"
            f"<ID>{report_name}</ID>"
            "</HEADER>"
            "<BODY><DESC><STATICVARIABLES>"
            f"{explode_flag}<SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>"
            "</STATICVARIABLES></DESC></BODY>"
            "</ENVELOPE>"
        )

    @staticmethod
    def build_collection_xml(name: str, object_type: str, fields: list[str] | None = None, company: str | None = None) -> str:
        methods = "".join(f"<NATIVEMETHOD>{field}</NATIVEMETHOD>" for field in fields) if fields else "<NATIVEMETHOD>*</NATIVEMETHOD>"
        company_xml = f"<SVCURRENTCOMPANY>{company}</SVCURRENTCOMPANY>" if company else ""
        return (
            "<ENVELOPE>"
            "<HEADER>"
            "<VERSION>1</VERSION>"
            "<TALLYREQUEST>EXPORT</TALLYREQUEST>"
            "<TYPE>COLLECTION</TYPE>"
            f"<ID>{name}</ID>"
            "</HEADER>"
            "<BODY><DESC>"
            f"<STATICVARIABLES>{company_xml}<SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>"
            "<TDL><TDLMESSAGE>"
            f'<COLLECTION NAME="{name}" ISINITIALIZE="Yes">'
            f"<TYPE>{object_type}</TYPE>"
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
            f"<SVCURRENTCOMPANY>{company}</SVCURRENTCOMPANY>"
            "<SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>"
            "</STATICVARIABLES>"
            "<TDL><TDLMESSAGE>"
            '<COLLECTION NAME="AllVouchers" ISINITIALIZE="Yes">'
            "<TYPE>Voucher</TYPE>"
            f"<FILTER>{filter_name}</FILTER>"
            "<FETCH>*, ALLLEDGERENTRIES, ALLINVENTORYENTRIES</FETCH>"
            "</COLLECTION>"
            f'<SYSTEM TYPE="Formulae" NAME="{filter_name}">{filter_formula}</SYSTEM>'
            "</TDLMESSAGE></TDL>"
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
