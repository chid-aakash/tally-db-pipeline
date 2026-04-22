from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from .config import get_settings
from .models import Base, CostCentre, Godown, Group, Ledger, StockGroup, StockItem, Unit, VoucherType


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
