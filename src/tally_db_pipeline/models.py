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


class SyncCheckpoint(Base):
    __tablename__ = "sync_checkpoints"
    __table_args__ = (UniqueConstraint("entity_type", "company_name", name="uq_sync_checkpoint_entity_company"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    entity_type: Mapped[str] = mapped_column(String(100), nullable=False)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime)
    last_sync_status: Mapped[str | None] = mapped_column(String(50))
    last_error_message: Mapped[str | None] = mapped_column(Text)
    last_row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_marker: Mapped[str | None] = mapped_column(String(255))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


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
    default_godown: Mapped[str | None] = mapped_column(String(255))
    last_synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class Group(Base):
    __tablename__ = "groups"
    __table_args__ = (UniqueConstraint("company_name", "name", name="uq_groups_company_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
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
    __table_args__ = (UniqueConstraint("company_name", "name", name="uq_ledgers_company_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
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
    __table_args__ = (UniqueConstraint("company_name", "name", name="uq_stock_groups_company_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    parent: Mapped[str | None] = mapped_column(String(255))
    guid: Mapped[str | None] = mapped_column(String(255))
    last_synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class StockItem(Base):
    __tablename__ = "stock_items"
    __table_args__ = (UniqueConstraint("company_name", "name", name="uq_stock_items_company_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
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
    __table_args__ = (UniqueConstraint("company_name", "name", name="uq_units_company_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    original_name: Mapped[str | None] = mapped_column(String(255))
    is_simple_unit: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    last_synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class Godown(Base):
    __tablename__ = "godowns"
    __table_args__ = (UniqueConstraint("company_name", "name", name="uq_godowns_company_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    parent: Mapped[str | None] = mapped_column(String(255))
    last_synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class CostCentre(Base):
    __tablename__ = "cost_centres"
    __table_args__ = (UniqueConstraint("company_name", "name", name="uq_cost_centres_company_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    parent: Mapped[str | None] = mapped_column(String(255))
    for_payroll: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_employee_group: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    last_synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class VoucherType(Base):
    __tablename__ = "voucher_types"
    __table_args__ = (UniqueConstraint("company_name", "name", name="uq_voucher_types_company_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
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
    alter_id: Mapped[int | None] = mapped_column(Integer, index=True)
    master_id: Mapped[int | None] = mapped_column(Integer, index=True)
    last_synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    inventory_entries: Mapped[list["VoucherInventoryEntry"]] = relationship(
        back_populates="voucher",
        cascade="all, delete-orphan",
    )
    ledger_entries: Mapped[list["VoucherLedgerEntry"]] = relationship(
        back_populates="voucher",
        cascade="all, delete-orphan",
    )
    unknown_sections: Mapped[list["VoucherUnknownSection"]] = relationship(
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


class VoucherUnknownSection(Base):
    __tablename__ = "voucher_unknown_sections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    voucher_id: Mapped[int] = mapped_column(ForeignKey("vouchers.id"), nullable=False)
    section_tag: Mapped[str] = mapped_column(String(255), nullable=False)
    section_xml: Mapped[str] = mapped_column(Text, nullable=False)

    voucher: Mapped["Voucher"] = relationship(back_populates="unknown_sections")


class SJPolicy(Base):
    __tablename__ = "sj_policy"
    __table_args__ = (UniqueConstraint("company_name", "voucher_type", name="uq_sj_policy_company_vt"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False)
    voucher_type: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    strict: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    rate_policy: Mapped[str] = mapped_column(String(50), default="stock_master", nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    groups: Mapped[list["SJPolicyGroup"]] = relationship(
        back_populates="policy", cascade="all, delete-orphan"
    )


class SJPolicyGroup(Base):
    __tablename__ = "sj_policy_group"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    policy_id: Mapped[int] = mapped_column(ForeignKey("sj_policy.id"), nullable=False)
    stock_group: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[str] = mapped_column(String(20), nullable=False)  # 'consume' | 'produce'
    default_godown: Mapped[str | None] = mapped_column(String(255))
    notes: Mapped[str | None] = mapped_column(Text)

    policy: Mapped["SJPolicy"] = relationship(back_populates="groups")


class ProductionEntry(Base):
    __tablename__ = "production_entries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    remote_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False)
    entry_date: Mapped[str] = mapped_column(String(20), nullable=False)  # YYYY-MM-DD
    voucher_type: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="draft", nullable=False)
    # draft | submitted | posted | failed
    narration: Mapped[str | None] = mapped_column(Text)
    tally_voucher_number: Mapped[str | None] = mapped_column(String(100))
    tally_master_id: Mapped[str | None] = mapped_column(String(100))
    tally_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime)
    posted_at: Mapped[datetime | None] = mapped_column(DateTime)

    lines: Mapped[list["ProductionEntryLine"]] = relationship(
        back_populates="entry", cascade="all, delete-orphan"
    )


class ProductionEntryLine(Base):
    __tablename__ = "production_entry_lines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    entry_id: Mapped[int] = mapped_column(ForeignKey("production_entries.id"), nullable=False)
    role: Mapped[str] = mapped_column(String(20), nullable=False)  # 'consume' | 'produce'
    item_name: Mapped[str] = mapped_column(String(255), nullable=False)
    quantity: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    uom: Mapped[str | None] = mapped_column(String(50))
    rate: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    amount: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    godown: Mapped[str | None] = mapped_column(String(255))
    opening_stock_snapshot: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)

    entry: Mapped["ProductionEntry"] = relationship(back_populates="lines")


class ConsumptionReportSelection(Base):
    """Persisted selection of stock groups / items to show on the daily consumption pivot."""

    __tablename__ = "consumption_report_selection"
    __table_args__ = (
        UniqueConstraint("company_name", "kind", "name", name="uq_consumption_selection"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False)
    kind: Mapped[str] = mapped_column(String(20), nullable=False)  # 'group' | 'item'
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class DailyProductionReport(Base):
    """Shift-level production tally: hourly output, rejection and rework grids plus idle-time log.

    Mirrors the paper "Daily Production Report" form. A report is the source-of-truth
    artifact for one (date, shift, line, model) combination; any Tally vouchers derived
    from it are a downstream action, not this table's concern."""

    __tablename__ = "daily_production_reports"
    __table_args__ = (
        UniqueConstraint(
            "company_name", "report_date", "shift", "line", "model",
            name="uq_dpr_shift_model",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False)
    report_date: Mapped[str] = mapped_column(String(10), nullable=False)  # YYYY-MM-DD, shift-start date
    shift: Mapped[str] = mapped_column(String(10), nullable=False)
    line: Mapped[str] = mapped_column(String(20), nullable=False)
    model: Mapped[str] = mapped_column(String(100), nullable=False, default="")
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="draft")  # draft|submitted
    # Hour slots for this report, serialized as JSON list of {key, label}. Lets each
    # shift carry its own schedule (S1 day slots differ from S2/S3 night slots) without
    # a schema change. If empty, the default S1 slots from daily_report.py apply.
    hour_slots_json: Mapped[str | None] = mapped_column(Text)
    rework_cleared_qty: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    narration: Mapped[str | None] = mapped_column(Text)
    supervisor_name: Mapped[str | None] = mapped_column(String(255))
    incharge_name: Mapped[str | None] = mapped_column(String(255))
    head_name: Mapped[str | None] = mapped_column(String(255))
    doc_no: Mapped[str | None] = mapped_column(String(50))
    rev_no: Mapped[str | None] = mapped_column(String(50))
    rev_date: Mapped[str | None] = mapped_column(String(20))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime)

    cells: Mapped[list["DPRHourlyCell"]] = relationship(back_populates="report", cascade="all, delete-orphan")
    idle_events: Mapped[list["DPRIdleEvent"]] = relationship(back_populates="report", cascade="all, delete-orphan")
    hour_models: Mapped[list["DPRHourModel"]] = relationship(back_populates="report", cascade="all, delete-orphan")


class DPRHourlyCell(Base):
    """One cell in any of the three hourly grids (production / rejection / rework).

    (section, row_key, hour_key) uniquely identifies a cell within a report. Row/hour
    keys are string slugs driven by the static definitions in production.py, so the
    schema does not need a migration when rows are added or removed."""

    __tablename__ = "dpr_hourly_cells"
    __table_args__ = (
        UniqueConstraint("report_id", "section", "row_key", "hour_key", name="uq_dpr_cell"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    report_id: Mapped[int] = mapped_column(ForeignKey("daily_production_reports.id"), nullable=False)
    section: Mapped[str] = mapped_column(String(20), nullable=False)  # 'production' | 'rejection' | 'rework'
    row_key: Mapped[str] = mapped_column(String(100), nullable=False)
    hour_key: Mapped[str] = mapped_column(String(20), nullable=False)
    value: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)

    report: Mapped["DailyProductionReport"] = relationship(back_populates="cells")


class ProductionProcess(Base):
    """A configurable row in the Daily Production Report grids.

    Replaces the hardcoded row lists in `daily_report.py`. Each row belongs to one
    line + section and carries a `stage` tag that lets monthly summaries roll up
    by workstation across lines whose labels differ ("Single Edger Output" vs
    "Workcenter Output" both share `stage='single_edger'` or `'work_center'`)."""

    __tablename__ = "production_processes"
    # No uniqueness on label: rejection sections legitimately repeat the same
    # label under different group headings ("Chip off / Breakage" appears under
    # both Single Edger and Auto Corner).

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False)
    line: Mapped[str] = mapped_column(String(20), nullable=False)
    section: Mapped[str] = mapped_column(String(20), nullable=False)  # production|rejection|rework
    stage: Mapped[str] = mapped_column(String(30), nullable=False)
    label: Mapped[str] = mapped_column(String(150), nullable=False)
    group_label: Mapped[str | None] = mapped_column(String(100))
    # 'input' or 'output' for production rows; None for rejection/rework.
    # Drives stage I/O accounting on the per-hour entry page.
    role: Mapped[str | None] = mapped_column(String(10))
    # When true, the stage's input - output delta must be allocated across
    # rejection rows before a per-hour entry can be saved. Disable for stages
    # that legitimately receive material from outside the line (e.g. Washing
    # also receives rejects from a Buffing rework loop).
    validate_count: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    sort_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )


class ShiftPresetSlot(Base):
    """One hour-column in a shift preset (per company, per shift S1/S2/S3).
    `key` is an immutable slug used as the DPR cell `hour_key`. Auto-seeded
    from `daily_report.SHIFT_PRESETS` on first use per (company, shift)."""

    __tablename__ = "shift_preset_slots"
    __table_args__ = (
        UniqueConstraint("company_name", "shift", "key", name="uq_sp_company_shift_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False)
    shift: Mapped[str] = mapped_column(String(10), nullable=False)
    key: Mapped[str] = mapped_column(String(20), nullable=False)
    label: Mapped[str] = mapped_column(String(40), nullable=False)
    sort_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )


class ProcessStage(Base):
    """User-editable workstation tag. `key` is an immutable slug so existing
    catalog/line rows that reference it keep resolving even if the label is
    renamed. Auto-seeded per company from `daily_report.STAGES` on first use."""

    __tablename__ = "process_stages"
    __table_args__ = (UniqueConstraint("company_name", "key", name="uq_ps_company_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False)
    key: Mapped[str] = mapped_column(String(50), nullable=False)
    label: Mapped[str] = mapped_column(String(100), nullable=False)
    sort_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )


class ProcessCatalogEntry(Base):
    """A reusable (section, label, stage, group) combo, defined once per company.

    Decoupled from `production_processes` so the same combo can be applied to
    multiple lines without re-typing label/stage/group. Applying a catalog entry
    to a line copies its values into a new `ProductionProcess` row — they are
    not linked thereafter, so editing the line row does not retroactively change
    the catalog (and vice versa)."""

    __tablename__ = "process_catalog"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False)
    section: Mapped[str] = mapped_column(String(20), nullable=False)
    label: Mapped[str] = mapped_column(String(150), nullable=False)
    stage: Mapped[str] = mapped_column(String(30), nullable=False)
    group_label: Mapped[str | None] = mapped_column(String(100))
    # 'input' or 'output' for production entries; None for rejection/rework.
    role: Mapped[str | None] = mapped_column(String(10))
    # See ProductionProcess.validate_count.
    validate_count: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class DPRHourModel(Base):
    """Model running in a particular hour of a DPR.

    Lets a single report capture a within-shift changeover: the report's
    `model` field is the primary/starting model; rows here override it for
    specific hours. Absence of a row for an hour means the primary model
    was running."""

    __tablename__ = "dpr_hour_models"
    __table_args__ = (
        UniqueConstraint("report_id", "hour_key", name="uq_dpr_hour_model"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    report_id: Mapped[int] = mapped_column(ForeignKey("daily_production_reports.id"), nullable=False)
    hour_key: Mapped[str] = mapped_column(String(20), nullable=False)
    model: Mapped[str] = mapped_column(String(100), nullable=False)

    report: Mapped["DailyProductionReport"] = relationship(back_populates="hour_models")


class LineDailyVoucherPost(Base):
    """Tracks the consolidated SJ - LINE voucher posted for one (company, date).

    One row per attempt at posting; the most recent successful row is the
    canonical Tally voucher for that day. Lets the Daily Production Report
    pages show 'already posted' state and link back to the Tally voucher
    number without re-querying Tally."""

    __tablename__ = "line_daily_voucher_posts"
    __table_args__ = (
        UniqueConstraint("company_name", "report_date", "remote_id", name="uq_ldvp_company_date_remote"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False)
    report_date: Mapped[str] = mapped_column(String(10), nullable=False)
    remote_id: Mapped[str] = mapped_column(String(150), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")  # pending|posted|failed
    scrap_rate: Mapped[float] = mapped_column(Float, default=5.50, nullable=False)
    tally_master_id: Mapped[str | None] = mapped_column(String(100))
    tally_voucher_number: Mapped[str | None] = mapped_column(String(100))
    tally_error: Mapped[str | None] = mapped_column(Text)
    posted_at: Mapped[datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class ProductionModelSpec(Base):
    """Physical spec of a cooktop-glass model: dimensions and burner-hole geometry.

    Used to convert hourly piece counts on a Daily Production Report into the
    weight-based scrap items (PLAIN GLASS SCRAP CLEAR / CIRCLE CULLET SCRAP CLEAR)
    that ship on the SJ - LINE Tally voucher. One row per (company, model)."""

    __tablename__ = "production_model_specs"
    __table_args__ = (UniqueConstraint("company_name", "model", name="uq_pms_company_model"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False)
    model: Mapped[str] = mapped_column(String(100), nullable=False)
    length_mm: Mapped[float] = mapped_column(Float, nullable=False)
    width_mm: Mapped[float] = mapped_column(Float, nullable=False)
    thickness_mm: Mapped[float] = mapped_column(Float, nullable=False)
    blank_price: Mapped[float | None] = mapped_column(Float)
    drilled_price: Mapped[float | None] = mapped_column(Float)
    printed_price: Mapped[float | None] = mapped_column(Float)
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    holes: Mapped[list["ProductionModelHole"]] = relationship(
        back_populates="spec", cascade="all, delete-orphan"
    )


class ProductionModelHole(Base):
    """One hole-diameter row for a model spec. A model can have any number
    of these — e.g. VEDA 3B = (165 mm × 2) + (139 mm × 1) + (10 mm × 4).
    Each row contributes its own cylinder of cullet to the SJ - LINE scrap
    weight: π·(diameter_mm/2)² × thickness_mm × density × count per drilled piece."""

    __tablename__ = "production_model_holes"
    __table_args__ = (
        UniqueConstraint("spec_id", "diameter_mm", name="uq_pmh_spec_diameter"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    spec_id: Mapped[int] = mapped_column(ForeignKey("production_model_specs.id"), nullable=False)
    diameter_mm: Mapped[float] = mapped_column(Float, nullable=False)
    count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    spec: Mapped["ProductionModelSpec"] = relationship(back_populates="holes")


class DPRIdleEvent(Base):
    """One idle-time incident (machine stop, glass shortage, etc.) on a shift."""

    __tablename__ = "dpr_idle_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    report_id: Mapped[int] = mapped_column(ForeignKey("daily_production_reports.id"), nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    machine: Mapped[str | None] = mapped_column(String(100))
    description: Mapped[str | None] = mapped_column(String(500))
    from_time: Mapped[str | None] = mapped_column(String(10))  # HH:MM
    to_time: Mapped[str | None] = mapped_column(String(10))
    time_loss_min: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    attended_by: Mapped[str | None] = mapped_column(String(100))
    remarks: Mapped[str | None] = mapped_column(String(500))

    report: Mapped["DailyProductionReport"] = relationship(back_populates="idle_events")
