"""FastAPI web app for production-entry data capture.

Flow:
  /                        -> pick company, date, voucher type (policy-driven)
  POST /entries            -> create draft entry, redirect to edit
  /entries/{id}/edit       -> render policy-allowed items as a table
  POST /entries/{id}/save  -> save lines
  /entries/{id}/review     -> voucher preview
  POST /entries/{id}/post  -> build voucher_dict, call TallyClient.import_voucher
  /entries/{id}            -> result view
  /entries                 -> list entries
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..config import get_settings
from ..db import get_session
from ..models import (
    Company,
    ProductionEntry,
    ProductionEntryLine,
    SJPolicy,
    StockItem,
)
from ..policy import (
    default_godown_for_role,
    entry_to_voucher_dict,
    generate_remote_id,
    get_policy,
    list_policies,
    resolve_items_for_role,
)
from ..tally_client import TallyClient


TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _inr(value) -> str:
    """Format a number as Indian Rupee with lakh/crore-style commas."""
    try:
        n = float(value or 0)
    except (TypeError, ValueError):
        return ""
    negative = n < 0
    n = abs(n)
    whole, _, frac = f"{n:.2f}".partition(".")
    if len(whole) <= 3:
        grouped = whole
    else:
        last3 = whole[-3:]
        rest = whole[:-3]
        # Group the rest in pairs from the right
        pairs = []
        while len(rest) > 2:
            pairs.insert(0, rest[-2:])
            rest = rest[:-2]
        if rest:
            pairs.insert(0, rest)
        grouped = ",".join(pairs) + "," + last3
    result = f"₹{grouped}.{frac}"
    return f"-{result}" if negative else result


def _nice_date(iso: str) -> str:
    """Format an ISO date (YYYY-MM-DD) as '24<sup>th</sup> April 26' (HTML)."""
    from markupsafe import Markup
    if not iso:
        return ""
    try:
        d = datetime.strptime(str(iso)[:10], "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return str(iso)
    n = d.day
    if 10 <= n % 100 <= 20:
        suf = "th"
    else:
        suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return Markup(
        f'<span style="white-space:nowrap">{n}<sup>{suf}</sup> {d.strftime("%B")} {d.strftime("%y")}</span>'
    )


templates.env.filters["inr"] = _inr
templates.env.filters["nice_date"] = _nice_date

app = FastAPI(title="Tally Production Entry")


def _session() -> Session:
    return get_session()


def _tally_client() -> TallyClient:
    settings = get_settings()
    return TallyClient(
        host=settings.tally_host,
        port=settings.tally_port,
        timeout=settings.tally_timeout_seconds,
        request_delay_ms=settings.tally_request_delay_ms,
        max_retries=settings.tally_max_retries,
        retry_backoff_ms=settings.tally_retry_backoff_ms,
        lock_file=settings.tally_lock_file,
        lock_stale_seconds=settings.tally_lock_stale_seconds,
    )


def _primary_uom(raw: str | None) -> str:
    """Strip any compound / conversion suffix from a UOM string.
    e.g. 'Mtrs = 2136.364 SQM' -> 'Mtrs'; 'No.' -> 'No.'."""
    if not raw:
        return ""
    s = str(raw).strip()
    for sep in [" = ", "=", " of "]:
        if sep in s:
            s = s.split(sep, 1)[0].strip()
            break
    return s


def _build_summary(session: Session, summary_date: str, company: str | None):
    """Group posted entries for (date) by company → voucher_type → item.
    Returns (companies, meta) where:
      companies = [{name, groups: [{voucher_type, rows, ...}], subtotal, voucher_count}]
    """
    sstmt = select(ProductionEntry).where(
        ProductionEntry.entry_date == summary_date,
        ProductionEntry.status == "posted",
    )
    if company:
        sstmt = sstmt.where(ProductionEntry.company_name == company)
    posted = list(session.scalars(sstmt))

    # company -> voucher_type -> item_name -> agg
    by_co: dict[str, dict[str, dict[str, dict]]] = {}
    vch_by_co_vt: dict[tuple[str, str], set] = {}
    count_by_co: dict[str, int] = {}
    total_amount = 0.0
    for e in posted:
        co = e.company_name
        vt = e.voucher_type
        bucket = by_co.setdefault(co, {}).setdefault(vt, {})
        vch_by_co_vt.setdefault((co, vt), set())
        if e.tally_voucher_number:
            vch_by_co_vt[(co, vt)].add(str(e.tally_voucher_number))
        count_by_co[co] = count_by_co.get(co, 0) + 1
        for l in e.lines:
            if not l.quantity or l.role != "consume":
                continue
            r = bucket.setdefault(l.item_name, {
                "item_name": l.item_name,
                "uom": _primary_uom(l.uom),
                "quantity": 0.0,
                "amount": 0.0,
                "godowns": set(),
                "vchs": set(),
            })
            r["quantity"] += float(l.quantity or 0)
            r["amount"] += float(l.amount or 0)
            total_amount += float(l.amount or 0)
            if l.godown: r["godowns"].add(l.godown)
            if e.tally_voucher_number: r["vchs"].add(str(e.tally_voucher_number))

    companies = []
    for co in sorted(by_co.keys()):
        groups = []
        for vt in sorted(by_co[co].keys()):
            items = [
                {
                    "item_name": r["item_name"],
                    "uom": r["uom"],
                    "quantity": r["quantity"],
                    "amount": r["amount"],
                    "rate_avg": (r["amount"] / r["quantity"]) if r["quantity"] else 0.0,
                    "godowns": ", ".join(sorted(r["godowns"])),
                    "vchs": ", ".join(sorted(r["vchs"])),
                }
                for r in sorted(by_co[co][vt].values(), key=lambda x: x["item_name"])
            ]
            groups.append({
                "voucher_type": vt,
                "voucher_numbers": ", ".join(sorted(vch_by_co_vt[(co, vt)])),
                "rows": items,
                "subtotal": sum(r["amount"] for r in items),
                "item_count": len(items),
            })
        companies.append({
            "name": co,
            "groups": groups,
            "subtotal": sum(g["subtotal"] for g in groups),
            "voucher_count": count_by_co[co],
        })
    meta = {
        "total_amount": total_amount,
        "voucher_count": len(posted),
        "company": company or (posted[0].company_name if posted else ""),
        "multi_company": len(by_co) > 1,
    }
    return companies, meta


def _list_companies(session: Session) -> list[str]:
    # Prefer companies that have policies loaded
    with_policy = set(
        session.scalars(select(SJPolicy.company_name).distinct())
    )
    all_companies = list(session.scalars(select(Company.name).order_by(Company.name)))
    if with_policy:
        return [c for c in all_companies if c in with_policy] or all_companies
    return all_companies


@app.get("/")
def root():
    return RedirectResponse("/app", status_code=307)


@app.get("/legacy", response_class=HTMLResponse)
def home(request: Request):
    with _session() as session:
        companies = _list_companies(session)
        company = companies[0] if companies else ""
        policies = list_policies(session, company) if company else []
    return templates.TemplateResponse(
        request,
        "home.html",
        {
            "companies": companies,
            "selected_company": company,
            "policies": policies,
            "today": date.today().isoformat(),
        },
    )


@app.post("/entries")
def create_entry(
    company: str = Form(...),
    entry_date: str = Form(...),
    voucher_type: str = Form(...),
    narration: str = Form(""),
):
    with _session() as session:
        policy = get_policy(session, company, voucher_type)
        if policy is None:
            raise HTTPException(400, f"No active policy for {company} / {voucher_type}")

        # Insert entry without remote_id first so we can derive it from id.
        entry = ProductionEntry(
            remote_id="pending",
            company_name=company,
            entry_date=entry_date,
            voucher_type=voucher_type,
            status="draft",
            narration=narration or None,
        )
        session.add(entry)
        session.flush()
        entry.remote_id = generate_remote_id(entry_date, voucher_type, entry.id)

        # Seed lines for all policy-allowed items (consume + produce) with qty=0
        for role in ("consume", "produce"):
            items = resolve_items_for_role(session, policy, role)
            godown = default_godown_for_role(policy, role)
            for item in items:
                session.add(
                    ProductionEntryLine(
                        entry_id=entry.id,
                        role=role,
                        item_name=item.name,
                        quantity=0.0,
                        uom=item.closing_uom or item.base_units or "No.",
                        rate=item.closing_rate or 0.0,
                        amount=0.0,
                        godown=godown,
                        opening_stock_snapshot=item.closing_quantity or 0.0,
                    )
                )
        session.commit()
        return RedirectResponse(f"/entries/{entry.id}/edit", status_code=303)


def _load_entry(session: Session, entry_id: int) -> ProductionEntry:
    entry = session.get(ProductionEntry, entry_id)
    if entry is None:
        raise HTTPException(404, "Entry not found")
    return entry


@app.get("/entries/{entry_id}/edit", response_class=HTMLResponse)
def edit_entry(request: Request, entry_id: int):
    with _session() as session:
        entry = _load_entry(session, entry_id)
        consume_lines = sorted(
            [l for l in entry.lines if l.role == "consume"], key=lambda l: l.item_name
        )
        produce_lines = sorted(
            [l for l in entry.lines if l.role == "produce"], key=lambda l: l.item_name
        )
    return templates.TemplateResponse(
        request,
        "entry.html",
        {
            "entry": entry,
            "consume_lines": consume_lines,
            "produce_lines": produce_lines,
        },
    )


@app.post("/entries/{entry_id}/save")
async def save_entry(entry_id: int, request: Request):
    form = await request.form()
    with _session() as session:
        entry = _load_entry(session, entry_id)
        if entry.status not in ("draft", "failed"):
            raise HTTPException(400, f"Cannot edit entry in status {entry.status}")

        narration = form.get("narration", "")
        entry.narration = narration or None

        for line in entry.lines:
            qty_raw = form.get(f"qty_{line.id}", "")
            rate_raw = form.get(f"rate_{line.id}", "")
            godown_raw = form.get(f"godown_{line.id}", "")
            try:
                qty = float(qty_raw) if qty_raw.strip() else 0.0
            except ValueError:
                qty = 0.0
            try:
                rate = float(rate_raw) if rate_raw.strip() else line.rate
            except ValueError:
                rate = line.rate
            line.quantity = qty
            line.rate = rate
            line.amount = round(qty * rate, 2)
            if godown_raw:
                line.godown = godown_raw

        session.commit()

        action = form.get("action", "save")
        if action == "review":
            return RedirectResponse(f"/entries/{entry_id}/review", status_code=303)
        return RedirectResponse(f"/entries/{entry_id}/edit", status_code=303)


@app.get("/entries/{entry_id}/review", response_class=HTMLResponse)
def review_entry(request: Request, entry_id: int):
    with _session() as session:
        entry = _load_entry(session, entry_id)
        nonzero = [l for l in entry.lines if l.quantity != 0]
        voucher_preview = entry_to_voucher_dict(entry)
    return templates.TemplateResponse(
        request,
        "review.html",
        {
            "entry": entry,
            "lines": sorted(nonzero, key=lambda l: (l.role, l.item_name)),
            "voucher_preview": voucher_preview,
        },
    )


@app.post("/entries/{entry_id}/post")
def post_entry(entry_id: int):
    with _session() as session:
        entry = _load_entry(session, entry_id)
        if entry.status == "posted":
            return RedirectResponse(f"/entries/{entry_id}", status_code=303)

        nonzero = [l for l in entry.lines if l.quantity != 0]
        if not nonzero:
            raise HTTPException(400, "No non-zero lines to post")

        voucher_dict = entry_to_voucher_dict(entry)
        entry.status = "submitted"
        entry.submitted_at = datetime.utcnow()
        session.commit()

        try:
            client = _tally_client()
            result = client.import_voucher(entry.company_name, voucher_dict, dry_run=False)
        except Exception as exc:
            entry.status = "failed"
            entry.tally_error = f"{type(exc).__name__}: {exc}"
            session.commit()
            return RedirectResponse(f"/entries/{entry_id}", status_code=303)

        if result.get("ok"):
            entry.status = "posted"
            entry.posted_at = datetime.utcnow()
            vch_id = result.get("last_vch_id")
            entry.tally_master_id = str(vch_id) if vch_id is not None else None
            vch_no = None
            if vch_id is not None:
                try:
                    vch_no = client.fetch_voucher_number_by_master_id(entry.company_name, vch_id)
                except Exception:
                    vch_no = None
            entry.tally_voucher_number = vch_no or (str(vch_id) if vch_id is not None else None)
            entry.tally_error = None
        else:
            entry.status = "failed"
            parts = []
            if result.get("line_error"):
                parts.append(f"LINEERROR: {result['line_error']}")
            if result.get("exception"):
                parts.append(f"EXCEPTIONS: {result['exception']}")
            parts.append(
                f"created={result.get('created')} altered={result.get('altered')} "
                f"ignored={result.get('ignored')} errors={result.get('errors')}"
            )
            entry.tally_error = " | ".join(parts)
        session.commit()
        return RedirectResponse(f"/entries/{entry_id}", status_code=303)


@app.get("/entries/{entry_id}", response_class=HTMLResponse)
def view_entry(request: Request, entry_id: int):
    with _session() as session:
        entry = _load_entry(session, entry_id)
        nonzero = sorted(
            [l for l in entry.lines if l.quantity != 0],
            key=lambda l: (l.role, l.item_name),
        )
    return templates.TemplateResponse(
        request,
        "result.html",
        {"entry": entry, "lines": nonzero},
    )


# ---------------------------------------------------------------------------
# JSON API (consumed by React SPA at /app)
# ---------------------------------------------------------------------------

class ApiLineIn(BaseModel):
    role: str  # 'consume' | 'produce'
    item_name: str
    quantity: float
    uom: str | None = None
    rate: float | None = None
    godown: str | None = None
    narration: str | None = None


class ApiEntryIn(BaseModel):
    company: str
    entry_date: str
    voucher_type: str
    narration: str | None = None
    lines: list[ApiLineIn]


@app.get("/api/companies")
def api_companies():
    with _session() as session:
        return {"companies": _list_companies(session)}


@app.get("/api/voucher-types")
def api_voucher_types(company: str):
    with _session() as session:
        policies = list_policies(session, company)
    return {
        "voucher_types": [
            {
                "voucher_type": p.voucher_type,
                "description": p.description,
                "strict": p.strict,
                "rate_policy": p.rate_policy,
            }
            for p in policies
        ]
    }


@app.get("/api/groups")
def api_groups(company: str, voucher_type: str, role: str = "consume"):
    """Return policy-allowed groups for a voucher type as a recursive tree.

    Each node shape:
      { stock_group, items: [...all items in subtree...], children: [node, ...] }

    Items at every level are the items in that node's full subtree, so the UI
    can scope the [+ Add item] dropdown at any level it chooses.
    """
    from ..models import StockGroup
    with _session() as session:
        policy = get_policy(session, company, voucher_type)
        if policy is None:
            raise HTTPException(404, f"No policy for {voucher_type}")

        all_sg = list(
            session.scalars(select(StockGroup).where(StockGroup.company_name == company))
        )
        children_of: dict[str, list[str]] = {}
        for sg in all_sg:
            if sg.parent and sg.parent != sg.name:
                children_of.setdefault(sg.parent, []).append(sg.name)

        # Pre-build direct-items index: parent_group_name -> [StockItem, ...]
        direct_items: dict[str, list[StockItem]] = {}
        for it in session.scalars(
            select(StockItem)
            .where(StockItem.company_name == company)
            .order_by(StockItem.name)
        ):
            if it.parent:
                direct_items.setdefault(it.parent, []).append(it)

        def build(node: str) -> dict:
            kids = [build(c) for c in sorted(children_of.get(node, []))]
            # subtree items = direct items at this node + every descendant's items
            subtree = [_item_json(i) for i in direct_items.get(node, [])]
            for k in kids:
                subtree.extend(k["items"])
            return {
                "stock_group": node,
                "items": subtree,
                "children": kids,
            }

        role_rows = [g for g in policy.groups if g.role == role]
        groups_out = []
        for g in role_rows:
            tree = build(g.stock_group)
            groups_out.append(
                {
                    "stock_group": g.stock_group,
                    "default_godown": g.default_godown,
                    "tree": tree,
                }
            )
    return {"voucher_type": voucher_type, "role": role, "groups": groups_out}


def _item_json(it: StockItem) -> dict:
    return {
        "name": it.name,
        "uom": it.closing_uom or it.base_units or "No.",
        "rate": it.closing_rate or 0.0,
        "opening_stock": it.closing_quantity or 0.0,
        "parent_group": it.parent,
    }


@app.post("/api/entries")
def api_create_entry(payload: ApiEntryIn):
    with _session() as session:
        policy = get_policy(session, payload.company, payload.voucher_type)
        if policy is None:
            raise HTTPException(400, f"No policy for {payload.voucher_type}")
        if not payload.lines:
            raise HTTPException(400, "At least one line is required")

        entry = ProductionEntry(
            remote_id="pending",
            company_name=payload.company,
            entry_date=payload.entry_date,
            voucher_type=payload.voucher_type,
            status="draft",
            narration=payload.narration,
        )
        session.add(entry)
        session.flush()
        entry.remote_id = generate_remote_id(payload.entry_date, payload.voucher_type, entry.id)

        # Lookup stock items by name for defaults (rate, uom)
        by_name = {
            it.name: it
            for it in session.scalars(
                select(StockItem).where(StockItem.company_name == payload.company)
            )
        }
        for line in payload.lines:
            item = by_name.get(line.item_name)
            uom = line.uom or (item.closing_uom or item.base_units or "No." if item else "No.")
            rate = line.rate if line.rate is not None else (item.closing_rate if item else 0.0)
            amount = round(line.quantity * (rate or 0.0), 2)
            godown = line.godown or default_godown_for_role(policy, line.role)
            # Per-line narration is stored in the shared narration by appending; Tally voucher
            # only has a single narration, so we collect per-line notes into the header.
            session.add(
                ProductionEntryLine(
                    entry_id=entry.id,
                    role=line.role,
                    item_name=line.item_name,
                    quantity=line.quantity,
                    uom=uom,
                    rate=rate or 0.0,
                    amount=amount,
                    godown=godown,
                    opening_stock_snapshot=item.closing_quantity if item else 0.0,
                    description=line.narration or None,
                )
            )
        # Keep entry.narration as the voucher-level narration only.
        entry.narration = payload.narration or None

        session.commit()
        return _entry_to_json(entry)


@app.get("/api/entries")
def api_list_entries(
    company: str | None = None,
    entry_date: str | None = None,
    status: str | None = None,
    limit: int = 100,
):
    with _session() as session:
        stmt = select(ProductionEntry).order_by(ProductionEntry.created_at.desc())
        if company:
            stmt = stmt.where(ProductionEntry.company_name == company)
        if entry_date:
            stmt = stmt.where(ProductionEntry.entry_date == entry_date)
        if status:
            stmt = stmt.where(ProductionEntry.status == status)
        entries = list(session.scalars(stmt.limit(limit)))
        return {"entries": [_entry_to_json(e) for e in entries]}


@app.get("/api/entries/{entry_id}")
def api_get_entry(entry_id: int):
    with _session() as session:
        entry = _load_entry(session, entry_id)
        return _entry_to_json(entry)


@app.post("/api/entries/{entry_id}/post")
def api_post_entry(entry_id: int):
    with _session() as session:
        entry = _load_entry(session, entry_id)
        if entry.status == "posted":
            return _entry_to_json(entry)

        nonzero = [l for l in entry.lines if l.quantity != 0]
        if not nonzero:
            raise HTTPException(400, "No non-zero lines to post")

        voucher_dict = entry_to_voucher_dict(entry)
        entry.status = "submitted"
        entry.submitted_at = datetime.utcnow()
        session.commit()

        try:
            client = _tally_client()
            result = client.import_voucher(entry.company_name, voucher_dict, dry_run=False)
        except Exception as exc:
            entry.status = "failed"
            entry.tally_error = f"{type(exc).__name__}: {exc}"
            session.commit()
            return _entry_to_json(entry)

        if result.get("ok"):
            entry.status = "posted"
            entry.posted_at = datetime.utcnow()
            vch_id = result.get("last_vch_id")
            entry.tally_master_id = str(vch_id) if vch_id is not None else None
            vch_no = None
            if vch_id is not None:
                try:
                    vch_no = client.fetch_voucher_number_by_master_id(entry.company_name, vch_id)
                except Exception:
                    vch_no = None
            entry.tally_voucher_number = vch_no or (str(vch_id) if vch_id is not None else None)
            entry.tally_error = None
        else:
            entry.status = "failed"
            parts = []
            if result.get("line_error"):
                parts.append(f"LINEERROR: {result['line_error']}")
            if result.get("exception"):
                parts.append(f"EXCEPTIONS: {result['exception']}")
            parts.append(
                f"created={result.get('created')} altered={result.get('altered')} "
                f"ignored={result.get('ignored')} errors={result.get('errors')}"
            )
            entry.tally_error = " | ".join(parts)
        session.commit()
        return _entry_to_json(entry)


def _entry_to_json(entry: ProductionEntry) -> dict:
    return {
        "id": entry.id,
        "remote_id": entry.remote_id,
        "company": entry.company_name,
        "voucher_type": entry.voucher_type,
        "entry_date": entry.entry_date,
        "status": entry.status,
        "narration": entry.narration,
        "tally_voucher_number": entry.tally_voucher_number,
        "tally_master_id": entry.tally_master_id,
        "tally_error": entry.tally_error,
        "lines": [
            {
                "id": l.id,
                "role": l.role,
                "item_name": l.item_name,
                "quantity": l.quantity,
                "uom": l.uom,
                "rate": l.rate,
                "amount": l.amount,
                "godown": l.godown,
                "description": l.description,
            }
            for l in entry.lines
        ],
        "created_at": entry.created_at.isoformat() if entry.created_at else None,
        "posted_at": entry.posted_at.isoformat() if entry.posted_at else None,
    }


def resolve_items_for_role_single_group(session: Session, company: str, group_root: str) -> list[StockItem]:
    """Resolve items under a single group (walks descendants)."""
    from ..policy import _descendant_group_names
    expanded = _descendant_group_names(session, company, [group_root])
    return list(
        session.scalars(
            select(StockItem)
            .where(StockItem.company_name == company, StockItem.parent.in_(expanded))
            .order_by(StockItem.name)
        )
    )


# ---------------------------------------------------------------------------
# React SPA at /app
# ---------------------------------------------------------------------------

STATIC_DIR = Path(__file__).parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/app", response_class=HTMLResponse)
def spa_react():
    index = STATIC_DIR / "app.html"
    if not index.exists():
        raise HTTPException(404, "React app not built. Expected at webapp/static/app.html")
    return FileResponse(
        index,
        headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"},
    )


@app.get("/entries", response_class=HTMLResponse)
def list_entries(
    request: Request,
    company: str | None = None,
    entry_date: str | None = None,
    summary_date: str | None = None,
):
    with _session() as session:
        stmt = select(ProductionEntry).order_by(ProductionEntry.created_at.desc())
        if company:
            stmt = stmt.where(ProductionEntry.company_name == company)
        if entry_date:
            stmt = stmt.where(ProductionEntry.entry_date == entry_date)
        entries = list(session.scalars(stmt))
        companies = _list_companies(session)

        # Distinct entry dates (across filters so sidebar reflects visible scope)
        dates_stmt = select(ProductionEntry.entry_date).distinct().order_by(ProductionEntry.entry_date.desc())
        if company:
            dates_stmt = dates_stmt.where(ProductionEntry.company_name == company)
        all_dates = [d for d in session.scalars(dates_stmt).all() if d]

        summary_companies: list[dict] = []
        summary_meta: dict = {}
        if summary_date:
            summary_companies, summary_meta = _build_summary(session, summary_date, company)

    return templates.TemplateResponse(
        request,
        "list.html",
        {
            "entries": entries,
            "companies": companies,
            "selected_company": company or "",
            "selected_date": entry_date or "",
            "all_dates": all_dates,
            "summary_date": summary_date or "",
            "summary_companies": summary_companies,
            "summary_meta": summary_meta,
        },
    )


def _logo_for_company(name: str) -> str | None:
    if not name:
        return None
    n = name.lower()
    if "avinash" in n:
        return "/static/logos/aapl.png"
    if "shanke" in n:
        return "/static/logos/sepl.png"
    return None


@app.get("/policies", response_class=HTMLResponse)
def policies_page(request: Request):
    """Serve the policy management SPA page."""
    path = Path(__file__).parent / "static" / "policies.html"
    if not path.exists():
        raise HTTPException(404, "policies.html missing")
    return FileResponse(
        path,
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


@app.get("/api/all-companies")
def api_all_companies():
    """All companies in the DB, including those without policies (for the policy page)."""
    with _session() as session:
        return {"companies": list(session.scalars(select(Company.name).order_by(Company.name)))}


@app.get("/api/company-default-godown")
def api_get_company_default_godown(company: str):
    with _session() as session:
        c = session.scalar(select(Company).where(Company.name == company))
        return {"default_godown": getattr(c, "default_godown", None) if c else None}


class CompanyDefaultGodownPayload(BaseModel):
    company: str
    default_godown: str | None = None


@app.put("/api/company-default-godown")
def api_put_company_default_godown(payload: CompanyDefaultGodownPayload):
    with _session() as session:
        c = session.scalar(select(Company).where(Company.name == payload.company))
        if c is None:
            raise HTTPException(404, f"Company not found: {payload.company}")
        c.default_godown = payload.default_godown or None
        session.commit()
        return {"ok": True, "default_godown": c.default_godown}


@app.get("/api/tally-status")
def api_tally_status():
    """Quick Tally-connectivity probe used by the UI status indicator."""
    import time, re
    from ..config import get_settings
    settings = get_settings()
    # Use a tight timeout so the UI stays responsive.
    client = TallyClient(
        host=settings.tally_host,
        port=settings.tally_port,
        timeout=3,
        request_delay_ms=0,
        max_retries=0,
        retry_backoff_ms=0,
        lock_file=settings.tally_lock_file,
        lock_stale_seconds=settings.tally_lock_stale_seconds,
    )
    xml = (
        "<ENVELOPE><HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST>"
        "<TYPE>Collection</TYPE><ID>List of Companies</ID></HEADER><BODY><DESC>"
        "<STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>"
        "<TDL><TDLMESSAGE><COLLECTION NAME=\"List of Companies\" ISMODIFY=\"No\">"
        "<TYPE>Company</TYPE><NATIVEMETHOD>Name</NATIVEMETHOD></COLLECTION>"
        "</TDLMESSAGE></TDL></DESC></BODY></ENVELOPE>"
    )
    started = time.monotonic()
    try:
        resp = client.post(xml)
    except Exception as exc:
        return {
            "ok": False,
            "host": f"{settings.tally_host}:{settings.tally_port}",
            "error": f"{type(exc).__name__}: {exc}",
            "latency_ms": int((time.monotonic() - started) * 1000),
            "companies": [],
        }
    latency = int((time.monotonic() - started) * 1000)
    companies = re.findall(r'<COMPANY[^>]*NAME="([^"]+)"', resp)
    return {
        "ok": True,
        "host": f"{settings.tally_host}:{settings.tally_port}",
        "latency_ms": latency,
        "companies": companies,
    }


@app.get("/api/godowns")
def api_godowns(company: str):
    from ..models import Godown
    with _session() as session:
        rows = session.scalars(
            select(Godown).where(Godown.company_name == company).order_by(Godown.name)
        )
        return {"godowns": [{"name": g.name, "parent": g.parent} for g in rows]}


@app.get("/api/policy-group-usage")
def api_policy_group_usage(company: str):
    """Returns { stock_group: [voucher_type, ...] } — groups already assigned
    to any policy in this company. Used by the policy editor to grey-out
    groups that belong to a different voucher type."""
    from ..models import SJPolicyGroup
    with _session() as session:
        rows = session.execute(
            select(SJPolicy.voucher_type, SJPolicyGroup.stock_group, SJPolicyGroup.role)
            .join(SJPolicyGroup, SJPolicyGroup.policy_id == SJPolicy.id)
            .where(SJPolicy.company_name == company)
        ).all()
        usage: dict[str, list[dict]] = {}
        for vt, sg, role in rows:
            usage.setdefault(sg, []).append({"voucher_type": vt, "role": role})
        return {"usage": usage}


@app.get("/api/tally-voucher-types")
def api_tally_voucher_types(company: str):
    """All voucher types synced from Tally for the company, with a flag indicating
    whether each already has a policy."""
    from ..models import VoucherType
    with _session() as session:
        vts = list(session.scalars(
            select(VoucherType).where(VoucherType.company_name == company).order_by(VoucherType.name)
        ))
        policy_names = set(session.scalars(
            select(SJPolicy.voucher_type).where(SJPolicy.company_name == company)
        ))
        return {
            "voucher_types": [
                {
                    "name": v.name,
                    "parent": v.parent,
                    "has_policy": v.name in policy_names,
                }
                for v in vts
            ]
        }


@app.get("/api/stock-group-tree")
def api_stock_group_tree(company: str):
    from ..models import StockGroup
    with _session() as session:
        all_sg = list(session.scalars(select(StockGroup).where(StockGroup.company_name == company)))
        children_of: dict[str, list[str]] = {}
        names = set()
        for sg in all_sg:
            names.add(sg.name)
            if sg.parent and sg.parent != sg.name:
                children_of.setdefault(sg.parent, []).append(sg.name)
        # Roots = groups whose parent is None or not in the set (e.g. "Primary")
        roots = sorted(sg.name for sg in all_sg if not sg.parent or sg.parent not in names)

        def build(name: str) -> dict:
            return {
                "name": name,
                "children": [build(c) for c in sorted(children_of.get(name, []))],
            }
        return {"tree": [build(r) for r in roots]}


class PolicyUpsertPayload(BaseModel):
    company: str
    voucher_type: str
    description: str | None = None
    strict: bool = True
    rate_policy: str = "stock_master"
    active: bool = True
    groups: list[dict]  # [{stock_group, role, default_godown?}]


@app.get("/api/policy")
def api_get_policy(company: str, voucher_type: str):
    with _session() as session:
        p = get_policy(session, company, voucher_type)
        if p is None:
            return {"exists": False, "voucher_type": voucher_type, "groups": []}
        return {
            "exists": True,
            "company": p.company_name,
            "voucher_type": p.voucher_type,
            "description": p.description,
            "strict": p.strict,
            "rate_policy": p.rate_policy,
            "active": p.active,
            "groups": [
                {"stock_group": g.stock_group, "role": g.role, "default_godown": g.default_godown}
                for g in p.groups
            ],
        }


@app.put("/api/policy")
def api_put_policy(payload: PolicyUpsertPayload):
    from ..models import SJPolicy, SJPolicyGroup
    with _session() as session:
        p = session.scalar(
            select(SJPolicy).where(
                SJPolicy.company_name == payload.company,
                SJPolicy.voucher_type == payload.voucher_type,
            )
        )
        if p is None:
            p = SJPolicy(company_name=payload.company, voucher_type=payload.voucher_type)
            session.add(p)
            session.flush()
        p.description = payload.description
        p.strict = payload.strict
        p.rate_policy = payload.rate_policy
        p.active = payload.active
        for row in list(p.groups):
            session.delete(row)
        session.flush()
        for g in payload.groups:
            sg = (g.get("stock_group") or "").strip()
            role = (g.get("role") or "").strip()
            if not sg or role not in ("consume", "produce"):
                continue
            session.add(SJPolicyGroup(
                policy_id=p.id,
                stock_group=sg,
                role=role,
                default_godown=(g.get("default_godown") or None),
            ))
        session.commit()
        return {"ok": True, "voucher_type": p.voucher_type, "group_count": len(payload.groups)}


@app.delete("/api/policy")
def api_delete_policy(company: str, voucher_type: str):
    from ..models import SJPolicy
    with _session() as session:
        p = session.scalar(
            select(SJPolicy).where(
                SJPolicy.company_name == company,
                SJPolicy.voucher_type == voucher_type,
            )
        )
        if p is None:
            raise HTTPException(404, "Policy not found")
        session.delete(p)
        session.commit()
        return {"ok": True}


@app.get("/summary/print", response_class=HTMLResponse)
def print_summary(request: Request, summary_date: str, company: str | None = None):
    with _session() as session:
        companies, meta = _build_summary(session, summary_date, company)
    return templates.TemplateResponse(
        request,
        "print_summary.html",
        {
            "summary_date": summary_date,
            "companies": companies,
            "meta": meta,
            "logo_left": "/static/logos/aapl.png",
            "logo_right": "/static/logos/sepl.png",
            "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        },
    )
