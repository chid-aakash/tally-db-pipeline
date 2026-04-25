"""Policy layer for voucher-type -> stock-group mapping.

Given a voucher type (e.g. "SJ - WHEELS"), resolve the set of stock items
allowed on the consume side and produce side by walking stock_groups
descendants of the policy's whitelisted groups.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import ProductionEntry, SJPolicy, SJPolicyGroup, StockGroup, StockItem


def load_policy_file(session: Session, path: Path) -> dict:
    """Upsert policies from a JSON file. Returns a summary dict."""
    data = json.loads(Path(path).read_text())
    company = data["company_name"]
    loaded = 0
    for spec in data["policies"]:
        policy = session.scalar(
            select(SJPolicy).where(
                SJPolicy.company_name == company,
                SJPolicy.voucher_type == spec["voucher_type"],
            )
        )
        if policy is None:
            policy = SJPolicy(company_name=company, voucher_type=spec["voucher_type"])
            session.add(policy)
            session.flush()
        policy.description = spec.get("description")
        policy.strict = bool(spec.get("strict", True))
        policy.rate_policy = spec.get("rate_policy", "stock_master")
        policy.active = bool(spec.get("active", True))

        # Replace group rows atomically
        for row in list(policy.groups):
            session.delete(row)
        session.flush()
        for grp in spec.get("groups", []):
            session.add(
                SJPolicyGroup(
                    policy_id=policy.id,
                    stock_group=grp["stock_group"],
                    role=grp["role"],
                    default_godown=grp.get("default_godown"),
                    notes=grp.get("notes"),
                )
            )
        loaded += 1
    session.commit()
    return {"company": company, "policies_loaded": loaded}


def get_policy(session: Session, company_name: str, voucher_type: str) -> SJPolicy | None:
    return session.scalar(
        select(SJPolicy).where(
            SJPolicy.company_name == company_name,
            SJPolicy.voucher_type == voucher_type,
            SJPolicy.active.is_(True),
        )
    )


def list_policies(session: Session, company_name: str) -> list[SJPolicy]:
    return list(
        session.scalars(
            select(SJPolicy)
            .where(SJPolicy.company_name == company_name, SJPolicy.active.is_(True))
            .order_by(SJPolicy.voucher_type)
        )
    )


def _descendant_group_names(session: Session, company_name: str, roots: Iterable[str]) -> set[str]:
    """Walk stock_groups children recursively. Returns the root names plus all descendants."""
    all_groups = {
        g.name: g.parent
        for g in session.scalars(
            select(StockGroup).where(StockGroup.company_name == company_name)
        )
    }
    # Build parent -> children index
    children: dict[str, list[str]] = {}
    for name, parent in all_groups.items():
        children.setdefault(parent or "", []).append(name)

    result: set[str] = set()
    stack = list(roots)
    while stack:
        node = stack.pop()
        if node in result:
            continue
        result.add(node)
        for child in children.get(node, []):
            stack.append(child)
    return result


def resolve_items_for_role(
    session: Session,
    policy: SJPolicy,
    role: str,
) -> list[StockItem]:
    """Resolve stock items whose parent-group (walked ancestrally) matches a
    policy_group row with the given role.
    """
    role_groups = [g.stock_group for g in policy.groups if g.role == role]
    if not role_groups:
        return []
    expanded = _descendant_group_names(session, policy.company_name, role_groups)
    items = session.scalars(
        select(StockItem)
        .where(
            StockItem.company_name == policy.company_name,
            StockItem.parent.in_(expanded),
        )
        .order_by(StockItem.name)
    ).all()
    return list(items)


def default_godown_for_role(policy: SJPolicy, role: str) -> str | None:
    for g in policy.groups:
        if g.role == role and g.default_godown:
            return g.default_godown
    return None


def generate_remote_id(entry_date: str, voucher_type: str, entry_id: int) -> str:
    """Stable client-side ID for Tally idempotency."""
    slug = voucher_type.replace(" ", "-").replace("/", "-").lower()
    return f"prodentry-{entry_date}-{slug}-{entry_id:06d}"


def entry_to_voucher_dict(entry: ProductionEntry) -> dict:
    """Convert a ProductionEntry to the voucher_dict shape consumed by TallyClient.import_voucher."""
    is_stock_journal = (
        entry.voucher_type == "Conversion Stock Journal"
        or entry.voucher_type.startswith("SJ ")
        or entry.voucher_type.startswith("SJ-")
        or "Stock Journal" in entry.voucher_type
    )

    inventory_entries = []
    for line in entry.lines:
        if line.quantity == 0:
            continue
        row = {
            "item_name": line.item_name,
            "quantity": line.quantity,
            "uom": line.uom or "No.",
            "godown": line.godown,
            "description": (line.description or "").strip() or None,
        }
        # Only include rate/amount if rate is non-zero. Zero-rate stock-journal
        # lines are silently rejected by Tally (EXCEPTIONS=1). Omitting both
        # tags lets Tally compute from its own stock valuation.
        has_rate = line.rate and line.rate > 0
        if has_rate:
            row["rate"] = line.rate
        if is_stock_journal:
            if line.role == "consume":
                row["direction"] = "out"
                if has_rate:
                    row["amount"] = abs(line.amount)
            else:
                row["direction"] = "in"
                if has_rate:
                    row["amount"] = -abs(line.amount)
        else:
            row["is_deemed_positive"] = line.role == "produce"
            if has_rate:
                row["amount"] = line.amount
        inventory_entries.append(row)

    voucher = {
        "voucher_type": entry.voucher_type,
        "date": entry.entry_date,
        "narration": entry.narration or "",
        "remote_id": entry.remote_id,
        "inventory_entries": inventory_entries,
    }
    if is_stock_journal:
        voucher["objview"] = "Consumption Voucher View"
    return voucher
