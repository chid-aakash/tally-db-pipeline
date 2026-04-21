from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class SyncRun(Base):
    __tablename__ = "sync_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sync_type: Mapped[str] = mapped_column(String(100), nullable=False)
    company_name: Mapped[str | None] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="running")
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime)
    error_message: Mapped[str | None] = mapped_column(Text)

    raw_payloads: Mapped[list["RawPayload"]] = relationship(back_populates="sync_run")


class RawPayload(Base):
    __tablename__ = "raw_payloads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sync_run_id: Mapped[int] = mapped_column(ForeignKey("sync_runs.id"), nullable=False)
    request_type: Mapped[str] = mapped_column(String(100), nullable=False)
    company_name: Mapped[str | None] = mapped_column(String(255))
    request_xml: Mapped[str] = mapped_column(Text, nullable=False)
    response_xml: Mapped[str] = mapped_column(Text, nullable=False)
    response_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    sync_run: Mapped["SyncRun"] = relationship(back_populates="raw_payloads")


class Company(Base):
    __tablename__ = "companies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    formal_name: Mapped[str | None] = mapped_column(String(255))
    currency_code: Mapped[str | None] = mapped_column(String(50))
    country: Mapped[str | None] = mapped_column(String(100))
    state_name: Mapped[str | None] = mapped_column(String(100))
    pincode: Mapped[str | None] = mapped_column(String(20))
    phone: Mapped[str | None] = mapped_column(String(100))
    email: Mapped[str | None] = mapped_column(String(255))
    gstn: Mapped[str | None] = mapped_column(String(100))
    income_tax_number: Mapped[str | None] = mapped_column(String(100))
    last_synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class Group(Base):
    __tablename__ = "groups"
    __table_args__ = (UniqueConstraint("name", name="uq_groups_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    parent: Mapped[str | None] = mapped_column(String(255))
    guid: Mapped[str | None] = mapped_column(String(255))
    is_revenue: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_deemed_positive: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    affects_gross_profit: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_subledger: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    last_synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class Ledger(Base):
    __tablename__ = "ledgers"
    __table_args__ = (UniqueConstraint("name", name="uq_ledgers_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    parent: Mapped[str | None] = mapped_column(String(255))
    guid: Mapped[str | None] = mapped_column(String(255))
    opening_balance: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    closing_balance: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    mailing_name: Mapped[str | None] = mapped_column(String(255))
    address: Mapped[str | None] = mapped_column(Text)
    state: Mapped[str | None] = mapped_column(String(100))
    country: Mapped[str | None] = mapped_column(String(100))
    pincode: Mapped[str | None] = mapped_column(String(20))
    email: Mapped[str | None] = mapped_column(String(255))
    phone: Mapped[str | None] = mapped_column(String(100))
    pan: Mapped[str | None] = mapped_column(String(100))
    gstin: Mapped[str | None] = mapped_column(String(100))
    gst_type: Mapped[str | None] = mapped_column(String(100))
    currency: Mapped[str | None] = mapped_column(String(50))
    is_bill_wise: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    affects_stock: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_by: Mapped[str | None] = mapped_column(String(255))
    last_synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class StockGroup(Base):
    __tablename__ = "stock_groups"
    __table_args__ = (UniqueConstraint("name", name="uq_stock_groups_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    parent: Mapped[str | None] = mapped_column(String(255))
    guid: Mapped[str | None] = mapped_column(String(255))
    last_synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class StockItem(Base):
    __tablename__ = "stock_items"
    __table_args__ = (UniqueConstraint("name", name="uq_stock_items_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    parent: Mapped[str | None] = mapped_column(String(255))
    base_units: Mapped[str | None] = mapped_column(String(100))
    opening_balance: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    opening_quantity: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    opening_rate: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    hsn_code: Mapped[str | None] = mapped_column(String(100))
    gst_applicable: Mapped[str | None] = mapped_column(String(100))
    guid: Mapped[str | None] = mapped_column(String(255))
    closing_quantity: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    closing_uom: Mapped[str | None] = mapped_column(String(100))
    closing_rate: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    closing_value: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    last_synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class Unit(Base):
    __tablename__ = "units"
    __table_args__ = (UniqueConstraint("name", name="uq_units_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    original_name: Mapped[str | None] = mapped_column(String(255))
    is_simple_unit: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    last_synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class Godown(Base):
    __tablename__ = "godowns"
    __table_args__ = (UniqueConstraint("name", name="uq_godowns_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    parent: Mapped[str | None] = mapped_column(String(255))
    last_synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class CostCentre(Base):
    __tablename__ = "cost_centres"
    __table_args__ = (UniqueConstraint("name", name="uq_cost_centres_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    parent: Mapped[str | None] = mapped_column(String(255))
    for_payroll: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_employee_group: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    last_synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class VoucherType(Base):
    __tablename__ = "voucher_types"
    __table_args__ = (UniqueConstraint("name", name="uq_voucher_types_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    parent: Mapped[str | None] = mapped_column(String(255))
    numbering_method: Mapped[str | None] = mapped_column(String(100))
    last_synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class Voucher(Base):
    __tablename__ = "vouchers"
    __table_args__ = (UniqueConstraint("guid", name="uq_vouchers_guid"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    guid: Mapped[str] = mapped_column(String(255), nullable=False)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False)
    voucher_type_name: Mapped[str] = mapped_column(String(255), nullable=False)
    base_voucher_type: Mapped[str | None] = mapped_column(String(255))
    voucher_date: Mapped[str | None] = mapped_column(String(20))
    voucher_number: Mapped[str | None] = mapped_column(String(100))
    party_name: Mapped[str | None] = mapped_column(String(255))
    narration: Mapped[str | None] = mapped_column(Text)
    party_gstin: Mapped[str | None] = mapped_column(String(100))
    place_of_supply: Mapped[str | None] = mapped_column(String(100))
    is_cancelled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_optional: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    last_synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    inventory_entries: Mapped[list["VoucherInventoryEntry"]] = relationship(
        back_populates="voucher",
        cascade="all, delete-orphan",
    )
    ledger_entries: Mapped[list["VoucherLedgerEntry"]] = relationship(
        back_populates="voucher",
        cascade="all, delete-orphan",
    )


class VoucherInventoryEntry(Base):
    __tablename__ = "voucher_inventory_entries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    voucher_id: Mapped[int] = mapped_column(ForeignKey("vouchers.id"), nullable=False)
    item_name: Mapped[str | None] = mapped_column(String(255))
    quantity: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    uom: Mapped[str | None] = mapped_column(String(100))
    rate: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    amount: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    hsn: Mapped[str | None] = mapped_column(String(100))
    ledger_name: Mapped[str | None] = mapped_column(String(255))
    ledger_amount: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    is_deemed_positive: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    gst_rates_json: Mapped[str | None] = mapped_column(Text)

    voucher: Mapped["Voucher"] = relationship(back_populates="inventory_entries")


class VoucherLedgerEntry(Base):
    __tablename__ = "voucher_ledger_entries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    voucher_id: Mapped[int] = mapped_column(ForeignKey("vouchers.id"), nullable=False)
    ledger_name: Mapped[str | None] = mapped_column(String(255))
    amount: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    is_deemed_positive: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_party_ledger: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    tax_rate: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    bill_allocations_json: Mapped[str | None] = mapped_column(Text)
    bank_allocations_json: Mapped[str | None] = mapped_column(Text)

    voucher: Mapped["Voucher"] = relationship(back_populates="ledger_entries")
