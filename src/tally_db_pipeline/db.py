from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from .config import get_settings
from .models import Base, CostCentre, Godown, Group, Ledger, StockGroup, StockItem, Unit, Voucher, VoucherType


settings = get_settings()
if settings.database_url.startswith("sqlite:///"):
    sqlite_path = settings.database_url.removeprefix("sqlite:///")
    Path(sqlite_path).parent.mkdir(parents=True, exist_ok=True)
connect_args = {"check_same_thread": False} if settings.database_url.startswith("sqlite") else {}
engine = create_engine(settings.database_url, future=True, connect_args=connect_args)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

COMPANY_SCOPED_TABLES = [
    "groups",
    "ledgers",
    "stock_groups",
    "stock_items",
    "units",
    "godowns",
    "cost_centres",
    "voucher_types",
]

COMPANY_SCOPED_MODELS = {
    "groups": Group,
    "ledgers": Ledger,
    "stock_groups": StockGroup,
    "stock_items": StockItem,
    "units": Unit,
    "godowns": Godown,
    "cost_centres": CostCentre,
    "voucher_types": VoucherType,
}


def ensure_runtime_schema() -> None:
    with engine.begin() as conn:
        inspector = inspect(conn)
        if inspector.has_table("vouchers"):
            columns = {column["name"] for column in inspector.get_columns("vouchers")}
            for column in ("remote_id", "voucher_key", "master_id"):
                if column not in columns:
                    conn.execute(text(f"ALTER TABLE vouchers ADD COLUMN {column} VARCHAR(255)"))
        for table_name in COMPANY_SCOPED_TABLES:
            inspector = inspect(conn)
            if not inspector.has_table(table_name):
                continue
            columns = {column["name"] for column in inspector.get_columns(table_name)}
            if "company_name" not in columns:
                conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN company_name VARCHAR(255) NOT NULL DEFAULT ''"))
                inspector = inspect(conn)
                columns = {column["name"] for column in inspector.get_columns(table_name)}
            if _needs_company_scope_rebuild(conn, table_name):
                _rebuild_company_scoped_table(conn, table_name, columns)
        if engine.dialect.name == "sqlite":
            _ensure_sqlite_runtime_indexes(conn)


def _ensure_sqlite_runtime_indexes(conn) -> None:
    indexes = [
        "CREATE INDEX IF NOT EXISTS ix_vouchers_company_type_date_number_guid ON vouchers (company_name, voucher_type_name, voucher_date, voucher_number, guid)",
        "CREATE INDEX IF NOT EXISTS ix_vouchers_master_id ON vouchers (master_id)",
        "CREATE INDEX IF NOT EXISTS ix_voucher_ledger_entries_voucher_id ON voucher_ledger_entries (voucher_id)",
        "CREATE INDEX IF NOT EXISTS ix_voucher_inventory_entries_voucher_id ON voucher_inventory_entries (voucher_id)",
        "CREATE INDEX IF NOT EXISTS ix_voucher_unknown_sections_voucher_id ON voucher_unknown_sections (voucher_id)",
        "CREATE INDEX IF NOT EXISTS ix_raw_payloads_request_type ON raw_payloads (request_type)",
        "CREATE INDEX IF NOT EXISTS ix_sync_runs_type_status_started ON sync_runs (sync_type, status, started_at)",
    ]
    existing_tables = {
        row[0]
        for row in conn.execute(text("SELECT name FROM sqlite_master WHERE type = 'table'")).all()
    }
    for statement in indexes:
        table_name = statement.split(" ON ", 1)[1].split(" ", 1)[0]
        if table_name in existing_tables:
            conn.execute(text(statement))


def _needs_company_scope_rebuild(conn, table_name: str) -> bool:
    if engine.dialect.name != "sqlite":
        return False
    create_sql = conn.execute(
        text("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = :name"),
        {"name": table_name},
    ).scalar_one_or_none() or ""
    normalized_sql = " ".join(create_sql.upper().split())
    return "UNIQUE (NAME)" in normalized_sql or "UNIQUE(NAME)" in normalized_sql


def _rebuild_company_scoped_table(conn, table_name: str, columns: set[str]) -> None:
    model = COMPANY_SCOPED_MODELS[table_name]
    temp_name = f"{table_name}__legacy"
    conn.execute(text(f"ALTER TABLE {table_name} RENAME TO {temp_name}"))
    model.__table__.create(conn)
    source_columns = [column.name for column in model.__table__.columns if column.name in columns]
    quoted_columns = ", ".join(source_columns)
    conn.execute(text(f"INSERT INTO {table_name} ({quoted_columns}) SELECT {quoted_columns} FROM {temp_name}"))
    conn.execute(text(f"DROP TABLE {temp_name}"))


def get_session() -> Session:
    return SessionLocal()
