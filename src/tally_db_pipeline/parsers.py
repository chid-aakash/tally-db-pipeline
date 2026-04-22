from __future__ import annotations

import re
import xml.etree.ElementTree as ET

_INVALID_XML_CHARS = re.compile(r"&#(?:[0-8]|1[0-1]|1[4-9]|2[0-9]|3[01]);")


def _clean_xml(text: str | bytes) -> str:
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="replace")
    text = _INVALID_XML_CHARS.sub("", text)
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
    return text.replace("\r", "")


def _attr(el: ET.Element, attr: str, default: str = "") -> str:
    value = el.get(attr, default)
    return value.strip().replace("\r", "") if value else default


def _text(el: ET.Element, tag: str, default: str = "") -> str:
    child = el.find(tag)
    if child is not None and child.text:
        return child.text.strip().replace("\r", "")
    return default


def _float(el: ET.Element, tag: str, default: float = 0.0) -> float:
    value = _text(el, tag, "")
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _bool(el: ET.Element, tag: str) -> bool:
    return _text(el, tag, "").lower() in {"yes", "true", "1"}


def _parse_qty_uom(raw: str) -> tuple[float, str]:
    if not raw or not raw.strip():
        return 0.0, ""
    parts = raw.strip().split()
    try:
        qty = float(parts[0])
    except (ValueError, IndexError):
        return 0.0, raw.strip()
    return qty, " ".join(parts[1:]) if len(parts) > 1 else ""


def _parse_rate_uom(raw: str) -> tuple[float, str]:
    if not raw or not raw.strip():
        return 0.0, ""
    if "/" in raw:
        amount, unit = raw.strip().split("/", 1)
        try:
            return float(amount), unit.strip()
        except ValueError:
            return 0.0, ""
    try:
        return float(raw.strip()), ""
    except ValueError:
        return 0.0, ""


def parse_company_collection(xml_string: str | bytes) -> list[dict]:
    root = ET.fromstring(_clean_xml(xml_string))
    companies: list[dict] = []
    for el in root.iter("COMPANY"):
        # Ignore CMPINFO counters like <COMPANY>0</COMPANY>; real company rows
        # are object nodes with either a NAME attr or child fields.
        name = _attr(el, "NAME") or _text(el, "NAME")
        if not name and not list(el):
            continue
        companies.append(
            {
                "name": name,
                "formal_name": _text(el, "FORMALNAME"),
                "currency_code": _text(el, "BASICCURRENCYCODE"),
                "country": _text(el, "COUNTRY"),
                "state_name": _text(el, "STATENAME"),
                "pincode": _text(el, "PINCODE"),
                "phone": _text(el, "PHONE"),
                "email": _text(el, "EMAIL"),
                "gstn": _text(el, "GSTN"),
                "income_tax_number": _text(el, "INCOMETAXNUMBER"),
            }
        )
    return companies


def parse_list_of_accounts(xml_string: str | bytes) -> dict:
    root = ET.fromstring(_clean_xml(xml_string))
    currencies: list[dict] = []
    groups: list[dict] = []
    ledgers: list[dict] = []
    static_variables = root.find("./BODY/DESC/STATICVARIABLES")
    company = _text(static_variables, "SVCURRENTCOMPANY", "") if static_variables is not None else ""

    for tally_message in root.iter("TALLYMESSAGE"):
        for child in tally_message:
            if child.tag == "COMPANY":
                company_name = _attr(child, "NAME") or _text(child, "NAME", "")
                if company_name:
                    company = company_name
            elif child.tag == "CURRENCY":
                currencies.append(
                    {
                        "name": _attr(child, "NAME"),
                        "mailing_name": _text(child, "MAILINGNAME"),
                        "expanded_symbol": _text(child, "EXPANDEDSYMBOL"),
                        "decimal_places": int(_float(child, "DECIMALPLACES", 2)),
                        "iso_code": _text(child, "ISOCURRENCYCODE"),
                    }
                )
            elif child.tag == "GROUP":
                groups.append(
                    {
                        "name": _attr(child, "NAME"),
                        "parent": _text(child, "PARENT"),
                        "guid": _text(child, "GUID"),
                        "is_revenue": _bool(child, "ISREVENUE"),
                        "is_deemed_positive": _bool(child, "ISDEEMEDPOSITIVE"),
                        "affects_gross_profit": _bool(child, "AFFECTSGROSSPROFIT"),
                        "is_subledger": _bool(child, "ISADDABLE"),
                    }
                )
            elif child.tag == "LEDGER":
                address_lines: list[str] = []
                for addr_list in child.iter("ADDRESS.LIST"):
                    for addr in addr_list:
                        if addr.text and addr.text.strip():
                            address_lines.append(addr.text.strip())

                mailing_name = _text(child, "MAILINGNAME")
                if not mailing_name:
                    for old in child.iter("OLDMAILINGNAME.LIST"):
                        for item in old:
                            if item.text and item.text.strip():
                                mailing_name = item.text.strip()
                                break

                ledgers.append(
                    {
                        "name": _attr(child, "NAME"),
                        "parent": _text(child, "PARENT"),
                        "guid": _text(child, "GUID"),
                        "opening_balance": _float(child, "OPENINGBALANCE"),
                        "closing_balance": _float(child, "CLOSINGBALANCE"),
                        "mailing_name": mailing_name or _attr(child, "NAME"),
                        "address": "\n".join(address_lines),
                        "state": _text(child, "LEDSTATENAME") or _text(child, "PRIORSTATENAME"),
                        "country": _text(child, "COUNTRYOFRESIDENCE", "India"),
                        "pincode": _text(child, "OLDPINCODE") or _text(child, "PINCODE"),
                        "email": _text(child, "EMAIL"),
                        "phone": _text(child, "LEDGERPHONE") or _text(child, "LEDGERMOBILE"),
                        "pan": _text(child, "INCOMETAXNUMBER"),
                        "gstin": _text(child, "GSTREGISTRATIONNUMBER") or _text(child, "PARTYGSTIN"),
                        "gst_type": _text(child, "GSTREGISTRATIONTYPE"),
                        "currency": _text(child, "CURRENCYNAME", "RS"),
                        "is_bill_wise": _bool(child, "ISBILLWISEON"),
                        "affects_stock": _bool(child, "AFFECTSSTOCK"),
                        "created_by": _text(child, "CREATEDBY"),
                    }
                )

    return {"company": company, "currencies": currencies, "groups": groups, "ledgers": ledgers}


def parse_collection(xml_string: str | bytes, entity_type: str) -> list[dict]:
    root = ET.fromstring(_clean_xml(xml_string))
    normalized_type = entity_type.upper().replace(" ", "")
    rows: list[dict] = []

    for el in root.iter(normalized_type):
        if normalized_type == "GROUP":
            rows.append(
                {
                    "name": _attr(el, "NAME") or _text(el, "NAME"),
                    "parent": _text(el, "PARENT"),
                    "guid": _text(el, "GUID"),
                    "is_revenue": _bool(el, "ISREVENUE"),
                    "is_deemed_positive": _bool(el, "ISDEEMEDPOSITIVE"),
                    "affects_gross_profit": _bool(el, "AFFECTSGROSSPROFIT"),
                    "is_subledger": _bool(el, "ISADDABLE"),
                }
            )
        elif normalized_type == "LEDGER":
            address_lines: list[str] = []
            for addr_list in el.iter("ADDRESS.LIST"):
                for addr in addr_list:
                    if addr.text and addr.text.strip():
                        address_lines.append(addr.text.strip())

            mailing_name = _text(el, "MAILINGNAME")
            if not mailing_name:
                for old in el.iter("OLDMAILINGNAME.LIST"):
                    for item in old:
                        if item.text and item.text.strip():
                            mailing_name = item.text.strip()
                            break

            rows.append(
                {
                    "name": _attr(el, "NAME") or _text(el, "NAME"),
                    "parent": _text(el, "PARENT"),
                    "guid": _text(el, "GUID"),
                    "opening_balance": _float(el, "OPENINGBALANCE"),
                    "closing_balance": _float(el, "CLOSINGBALANCE"),
                    "mailing_name": mailing_name or _attr(el, "NAME") or _text(el, "NAME"),
                    "address": "\n".join(address_lines),
                    "state": _text(el, "LEDSTATENAME") or _text(el, "PRIORSTATENAME"),
                    "country": _text(el, "COUNTRYOFRESIDENCE", "India"),
                    "pincode": _text(el, "OLDPINCODE") or _text(el, "PINCODE"),
                    "email": _text(el, "EMAIL"),
                    "phone": _text(el, "LEDGERPHONE") or _text(el, "LEDGERMOBILE"),
                    "pan": _text(el, "INCOMETAXNUMBER"),
                    "gstin": _text(el, "GSTREGISTRATIONNUMBER") or _text(el, "PARTYGSTIN"),
                    "gst_type": _text(el, "GSTREGISTRATIONTYPE"),
                    "currency": _text(el, "CURRENCYNAME", "RS"),
                    "is_bill_wise": _bool(el, "ISBILLWISEON"),
                    "affects_stock": _bool(el, "AFFECTSSTOCK"),
                    "created_by": _text(el, "CREATEDBY"),
                }
            )
        elif normalized_type == "STOCKGROUP":
            rows.append({"name": _attr(el, "NAME") or _text(el, "NAME"), "parent": _text(el, "PARENT"), "guid": _text(el, "GUID")})
        elif normalized_type == "STOCKITEM":
            rows.append(
                {
                    "name": _attr(el, "NAME") or _text(el, "NAME"),
                    "parent": _text(el, "PARENT"),
                    "base_units": _text(el, "BASEUNITS"),
                    "opening_balance": _float(el, "OPENINGBALANCE"),
                    "opening_quantity": _float(el, "OPENINGQUANTITY"),
                    "opening_rate": _float(el, "OPENINGRATE"),
                    "hsn_code": _text(el, "HSNCODE") or _text(el, "GSTHSN"),
                    "gst_applicable": _text(el, "GSTAPPLICABLE"),
                    "guid": _text(el, "GUID"),
                }
            )
        elif normalized_type == "UNIT":
            rows.append(
                {
                    "name": _attr(el, "NAME") or _text(el, "NAME"),
                    "original_name": _text(el, "ORIGINALNAME"),
                    "is_simple_unit": _bool(el, "ISSIMPLEUNIT"),
                }
            )
        elif normalized_type == "GODOWN":
            rows.append({"name": _attr(el, "NAME") or _text(el, "NAME"), "parent": _text(el, "PARENT")})
        elif normalized_type == "COSTCENTRE":
            rows.append(
                {
                    "name": _attr(el, "NAME") or _text(el, "NAME"),
                    "parent": _text(el, "PARENT"),
                    "for_payroll": _bool(el, "FORPAYROLL"),
                    "is_employee_group": _bool(el, "ISEMPLOYEEGROUP"),
                }
            )
        elif normalized_type == "VOUCHERTYPE":
            rows.append(
                {
                    "name": _attr(el, "NAME") or _text(el, "NAME"),
                    "parent": _text(el, "PARENT"),
                    "numbering_method": _text(el, "NUMBERINGMETHOD"),
                }
            )
        else:
            raise ValueError(f"Unsupported entity type: {entity_type}")

    return rows


def parse_stock_item_balances(xml_string: str | bytes) -> list[dict]:
    root = ET.fromstring(_clean_xml(xml_string))
    rows: list[dict] = []
    for el in root.iter("STOCKITEM"):
        name = _attr(el, "NAME") or _text(el, "NAME")
        if not name:
            continue
        quantity, uom = _parse_qty_uom(_text(el, "CLOSINGBALANCE", "0"))
        rate, _ = _parse_rate_uom(_text(el, "CLOSINGRATE", "0"))
        rows.append(
            {
                "name": name,
                "parent": _text(el, "PARENT"),
                "closing_quantity": quantity,
                "closing_uom": uom,
                "closing_rate": rate,
                "closing_value": abs(_float(el, "CLOSINGVALUE")),
            }
        )
    return rows


def parse_vouchers(xml_string: str | bytes) -> list[dict]:
    root = ET.fromstring(_clean_xml(xml_string))
    vouchers: list[dict] = []

    for el in root.iter("VOUCHER"):
        raw_date = _text(el, "DATE", "")
        voucher_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}" if len(raw_date) == 8 else ""
        voucher = {
            "voucher_type_name": el.get("VCHTYPE", "") or _text(el, "VOUCHERTYPENAME"),
            "voucher_date": voucher_date,
            "voucher_number": _text(el, "VOUCHERNUMBER"),
            "party_name": _text(el, "PARTYLEDGERNAME") or _text(el, "PARTYNAME"),
            "narration": _text(el, "NARRATION"),
            "party_gstin": _text(el, "PARTYGSTIN"),
            "place_of_supply": _text(el, "PLACEOFSUPPLY"),
            "is_cancelled": _bool(el, "ISCANCELLED"),
            "is_optional": _bool(el, "ISOPTIONAL"),
            "guid": _text(el, "GUID"),
            "inventory_entries": [],
            "ledger_entries": [],
            "unknown_sections": [],
        }

        for inv_el in el.findall("ALLINVENTORYENTRIES.LIST"):
            item_name = _text(inv_el, "STOCKITEMNAME")
            if not item_name:
                continue
            qty, uom = _parse_qty_uom(_text(inv_el, "ACTUALQTY") or _text(inv_el, "BILLEDQTY", "0"))
            rate, _ = _parse_rate_uom(_text(inv_el, "RATE", "0"))
            alloc = inv_el.find("ACCOUNTINGALLOCATIONS.LIST")
            ledger_name = _text(alloc, "LEDGERNAME") if alloc is not None else ""
            ledger_amount = _float(alloc, "AMOUNT") if alloc is not None else 0.0
            gst_rates: dict[str, float] = {}
            for rate_detail in inv_el.findall("RATEDETAILS.LIST"):
                duty_head = _text(rate_detail, "GSTRATEDUTYHEAD")
                gst_rate = _float(rate_detail, "GSTRATE")
                if duty_head and gst_rate:
                    gst_rates[duty_head] = gst_rate
            voucher["inventory_entries"].append(
                {
                    "item_name": item_name,
                    "quantity": qty,
                    "uom": uom,
                    "rate": rate,
                    "amount": _float(inv_el, "AMOUNT"),
                    "hsn": _text(inv_el, "GSTHSNNAME"),
                    "ledger_name": ledger_name,
                    "ledger_amount": ledger_amount,
                    "is_deemed_positive": _bool(inv_el, "ISDEEMEDPOSITIVE"),
                    "gst_rates": gst_rates,
                }
            )

        # ALLLEDGERENTRIES is the fuller form. When present, Tally may also emit
        # LEDGERENTRIES, which would double-count the same rows if we read both.
        ledger_lists = el.findall("ALLLEDGERENTRIES.LIST")
        if not ledger_lists:
            ledger_lists = el.findall("LEDGERENTRIES.LIST")
        for led_el in ledger_lists:
            ledger_name = _text(led_el, "LEDGERNAME")
            if not ledger_name:
                continue
            bill_allocations: list[dict] = []
            for alloc in led_el.findall("BILLALLOCATIONS.LIST"):
                name = _text(alloc, "NAME")
                if not name:
                    continue
                bill_allocations.append({"name": name, "type": _text(alloc, "BILLTYPE"), "amount": _float(alloc, "AMOUNT")})

            bank_allocations: list[dict] = []
            for alloc in led_el.findall("BANKALLOCATIONS.LIST"):
                bank_party = _text(alloc, "BANKPARTYNAME") or _text(alloc, "PAYMENTFAVOURING")
                if not bank_party:
                    continue
                bank_allocations.append(
                    {
                        "party": bank_party,
                        "transaction_type": _text(alloc, "TRANSACTIONTYPE"),
                        "instrument_number": _text(alloc, "INSTRUMENTNUMBER"),
                        "amount": _float(alloc, "AMOUNT"),
                    }
                )

            tax_rate = 0.0
            rate_list = led_el.find("RATEOFINVOICETAX.LIST")
            if rate_list is not None:
                for child in rate_list:
                    if child.text:
                        try:
                            tax_rate = float(child.text.strip())
                        except ValueError:
                            pass

            voucher["ledger_entries"].append(
                {
                    "ledger_name": ledger_name,
                    "amount": _float(led_el, "AMOUNT"),
                    "is_deemed_positive": _bool(led_el, "ISDEEMEDPOSITIVE"),
                    "is_party_ledger": _bool(led_el, "ISPARTYLEDGER"),
                    "tax_rate": tax_rate,
                    "bill_allocations": bill_allocations,
                    "bank_allocations": bank_allocations,
                }
            )

        known_tags = {
            "DATE",
            "GUID",
            "NARRATION",
            "PARTYGSTIN",
            "PLACEOFSUPPLY",
            "VOUCHERNUMBER",
            "VOUCHERTYPENAME",
            "PARTYLEDGERNAME",
            "PARTYNAME",
            "ISCANCELLED",
            "ISOPTIONAL",
            "ALLINVENTORYENTRIES.LIST",
            "ALLLEDGERENTRIES.LIST",
            "LEDGERENTRIES.LIST",
        }
        for child in el:
            if child.tag in known_tags:
                continue
            if child.tag.endswith(".LIST") or list(child):
                voucher["unknown_sections"].append(
                    {
                        "tag": child.tag,
                        "xml": ET.tostring(child, encoding="unicode"),
                    }
                )

        vouchers.append(voucher)

    return vouchers


def resolve_voucher_base_type(voucher_type_name: str, voucher_types: list[dict]) -> str:
    by_name = {row["name"]: row for row in voucher_types}
    base_types = {
        "Sales",
        "Purchase",
        "Receipt",
        "Payment",
        "Journal",
        "Contra",
        "Credit Note",
        "Debit Note",
        "Sales Order",
        "Purchase Order",
        "Delivery Note",
        "Receipt Note",
        "Stock Journal",
        "Physical Stock",
        "Memorandum",
        "Rejections In",
        "Rejections Out",
        "Payroll",
    }

    current = voucher_type_name
    visited: set[str] = set()
    while current and current not in visited:
        if current in base_types:
            return current
        visited.add(current)
        row = by_name.get(current)
        if not row or not row.get("parent"):
            break
        current = row["parent"]

    lower = voucher_type_name.lower()
    for base in base_types:
        if base.lower() in lower:
            return base
    return voucher_type_name
