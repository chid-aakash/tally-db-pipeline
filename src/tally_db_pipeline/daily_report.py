"""Static layout for the Daily Production Report (Avinash Line 4, Renz 3b).

Row and hour keys used here are the canonical identifiers persisted in
`dpr_hourly_cells.row_key` / `hour_key`. Rendering order is driven by this
module so changes here flow straight through to both the edit and summary
templates without a DB migration."""

from __future__ import annotations

from dataclasses import dataclass


# Hourly slots as they appear on the paper form. Keys are slugs so they are
# URL-/form-safe; labels are what the user sees.
HOUR_SLOTS: list[tuple[str, str]] = [
    ("h08", "8"),
    ("h09", "9"),
    ("h10", "10"),
    ("h11", "11"),
    ("h12", "12"),
    ("h13", "1"),
    ("h1430", "2:30"),
    ("h1530", "3:30"),
    ("h1630", "4:30"),
    ("h1730", "5:30"),
    ("h1830", "6:30"),
    ("h1930", "7:30"),
    ("ot", "OT"),
]


@dataclass(frozen=True)
class Row:
    key: str
    label: str
    group: str | None = None  # Process stage heading shown on the paper form.


# ---- Section 1: Cumulative Production + Hourly Output Monitoring ----
PRODUCTION_ROWS: list[Row] = [
    Row("single_edger_input", "Single Edger Input"),
    Row("single_edger_output", "Single Edger Output"),
    Row("auto_corner_output", "Auto Corner Output"),
    Row("drilling_output", "Drilling Output"),
    Row("washing_incl_rework", "Washing (Incl Rework)"),
]

# ---- Section 2: Cumulative Rejection + Hourly Rejection Monitoring ----
REJECTION_ROWS: list[Row] = [
    Row("rej_se_chipoff", "Chip off / Breakage", group="Single Edger"),
    Row("rej_se_overgrind", "Over Grinding", group="Single Edger"),
    Row("rej_se_cnc", "CNC Defects", group="Single Edger"),
    Row("rej_ac_chipoff", "Chip off / Breakage", group="Auto Corner"),
    Row("rej_ac_overgrind", "Over Grinding", group="Auto Corner"),
    Row("rej_dr_burner_breakage", "Burner Hole Breakage", group="Drilling"),
    Row("rej_dr_burner_chipoff", "Burner Hole Chip off", group="Drilling"),
    Row("rej_dr_burner_offset", "Burner Hole Offset", group="Drilling"),
    Row("rej_dr_knob_breakage", "Knob Hole Breakage", group="Drilling"),
    Row("rej_dr_knob_chipoff", "Knob Hole Chip off", group="Drilling"),
    Row("rej_dr_knob_offset", "Knob Hole Offset", group="Drilling"),
    Row("rej_dr_leg_breakage", "Leg Hole Breakage", group="Drilling"),
    Row("rej_dr_leg_chipoff", "Leg Hole Chip off", group="Drilling"),
    Row("rej_dr_leg_offset", "Leg Hole Offset", group="Drilling"),
    Row("rej_dr_handling", "Handling Damage", group="Drilling"),
    Row("rej_wa_deep_scratch", "Deep Scratch", group="Washing"),
    Row("rej_wa_rg_bubbles", "RG Bubbles", group="Washing"),
    Row("rej_wa_others", "Others", group="Washing"),
]

# ---- Section 3: Cumulative Rework + Hourly Rework Monitoring ----
REWORK_ROWS: list[Row] = [
    Row("rw_single_side", "Single Side Scratch"),
    Row("rw_double_side", "Double Side Scratch"),
    Row("rw_hole_chipoff", "Hole Chip off"),
    Row("rw_edge_chipoff", "Edge Chip off"),
    Row("rw_auto_corner_radius", "Auto Corner Radius"),
]


SECTIONS: dict[str, list[Row]] = {
    "production": PRODUCTION_ROWS,
    "rejection": REJECTION_ROWS,
    "rework": REWORK_ROWS,
}


SHIFT_OPTIONS: list[str] = ["A", "B", "G"]
LINE_OPTIONS: list[str] = ["1", "2", "3", "4"]


def rejection_rows_grouped() -> list[tuple[str, list[Row]]]:
    """Preserve the on-form grouping order so the template renders process
    headings exactly like the paper (Single Edger → Auto Corner → Drilling → Washing)."""
    out: list[tuple[str, list[Row]]] = []
    seen: dict[str, int] = {}
    for r in REJECTION_ROWS:
        g = r.group or ""
        if g not in seen:
            seen[g] = len(out)
            out.append((g, []))
        out[seen[g]][1].append(r)
    return out


def empty_cells_matrix() -> dict[str, dict[str, dict[str, float]]]:
    """Return `{section: {row_key: {hour_key: 0.0}}}` with every slot zeroed.

    Used by templates so missing DB rows still render as 0 rather than blanks —
    avoids a KeyError the first time a report is opened."""
    out: dict[str, dict[str, dict[str, float]]] = {}
    for section, rows in SECTIONS.items():
        out[section] = {
            r.key: {h[0]: 0.0 for h in HOUR_SLOTS} for r in rows
        }
    return out


def row_totals(cells: dict[str, dict[str, float]]) -> dict[str, float]:
    """Sum hour values per row_key, returning `{row_key: total}`."""
    return {rk: sum(vals.values()) for rk, vals in cells.items()}


def column_totals(cells: dict[str, dict[str, float]]) -> dict[str, float]:
    """Sum per hour across all rows in a section."""
    out: dict[str, float] = {h[0]: 0.0 for h in HOUR_SLOTS}
    for _rk, vals in cells.items():
        for hk, v in vals.items():
            out[hk] = out.get(hk, 0.0) + v
    return out


def grand_total(cells: dict[str, dict[str, float]]) -> float:
    return sum(v for vals in cells.values() for v in vals.values())


def safe_pct(numer: float, denom: float) -> float:
    return (numer / denom * 100.0) if denom else 0.0
