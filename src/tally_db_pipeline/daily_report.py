"""Static layout for the Daily Production Report (Avinash Line 4, Renz 3b).

Row and hour keys used here are the canonical identifiers persisted in
`dpr_hourly_cells.row_key` / `hour_key`. Rendering order is driven by this
module so changes here flow straight through to both the edit and summary
templates without a DB migration."""

from __future__ import annotations

from dataclasses import dataclass


# Reserved hour_key for per-row cumulative totals stored in DPRHourlyCell.
# Keeps the schema uniform rather than special-casing cumulative columns on
# the parent report.
CUMULATIVE_KEY = "cumulative"


# Hourly slots as they appear on the paper form (S1 day shift — matches the Line 4
# Renz 3b report image). Keys are slugs so they are URL-/form-safe; labels are what
# the user sees. `HOUR_SLOTS` is the fallback default when a report has no
# `hour_slots_json` stored.
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


# Shift presets. Users can pick one at report-create time and edit the labels per
# report — the edited list is persisted in `DailyProductionReport.hour_slots_json`
# so historical reports always render with their original hour scheme even if the
# presets here change later.
SHIFT_PRESETS: dict[str, list[tuple[str, str]]] = {
    "S1": HOUR_SLOTS,
    "S2": [
        ("h16", "4"),
        ("h17", "5"),
        ("h18", "6"),
        ("h19", "7"),
        ("h20", "8"),
        ("h21", "9"),
        ("h2230", "10:30"),
        ("h2330", "11:30"),
        ("h0030", "12:30"),
        ("h0130", "1:30"),
        ("h0230", "2:30"),
        ("h0330", "3:30"),
        ("ot", "OT"),
    ],
    "S3": [
        ("h04", "4"),
        ("h05", "5"),
        ("h06", "6"),
        ("h07", "7"),
        ("h08", "8"),
        ("h09", "9"),
        ("h1030", "10:30"),
        ("h1130", "11:30"),
        ("h1230", "12:30"),
        ("h1330", "1:30"),
        ("h1430", "2:30"),
        ("h1530", "3:30"),
        ("ot", "OT"),
    ],
}


def load_hour_slots(hour_slots_json: str | None, shift: str | None = None) -> list[tuple[str, str]]:
    """Return the effective hour list for a report.

    Prefer the report's stored JSON; fall back to the shift preset; finally the
    legacy default. Returned as list[(key, label)] so templates can iterate
    without caring about shape."""
    import json

    if hour_slots_json:
        try:
            data = json.loads(hour_slots_json)
            return [(str(s["key"]), str(s["label"])) for s in data if s.get("key")]
        except (ValueError, KeyError, TypeError):
            pass  # Fall through to preset
    if shift and shift in SHIFT_PRESETS:
        return list(SHIFT_PRESETS[shift])
    return list(HOUR_SLOTS)


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


SHIFT_OPTIONS: list[str] = ["S1", "S2", "S3"]
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


def empty_cells_matrix(
    hour_slots: list[tuple[str, str]] | None = None,
) -> dict[str, dict[str, dict[str, float]]]:
    """Return `{section: {row_key: {hour_key: 0.0}}}` with every slot zeroed.

    `hour_slots` is the report's effective hour list (from `load_hour_slots`).
    Includes the reserved `CUMULATIVE_KEY` cell on every row so templates can
    always render the Cumulative column uniformly."""
    slots = hour_slots if hour_slots is not None else HOUR_SLOTS
    hour_keys = [CUMULATIVE_KEY] + [h[0] for h in slots]
    out: dict[str, dict[str, dict[str, float]]] = {}
    for section, rows in SECTIONS.items():
        out[section] = {r.key: {hk: 0.0 for hk in hour_keys} for r in rows}
    return out


def row_totals(cells: dict[str, dict[str, float]]) -> dict[str, float]:
    """Sum of hourly values per row. Excludes the cumulative cell so the
    "Total" column on the form (sum of the shift's hours) stays distinct from
    the "Cumulative" column (running total from model start)."""
    return {
        rk: sum(v for hk, v in vals.items() if hk != CUMULATIVE_KEY)
        for rk, vals in cells.items()
    }


def column_totals(
    cells: dict[str, dict[str, float]],
    hour_slots: list[tuple[str, str]] | None = None,
) -> dict[str, float]:
    """Sum per hour across all rows in a section. Cumulative cells are summed
    separately so the footer row shows a total cumulative alongside hourly totals."""
    slots = hour_slots if hour_slots is not None else HOUR_SLOTS
    hour_keys = [CUMULATIVE_KEY] + [h[0] for h in slots]
    out: dict[str, float] = {hk: 0.0 for hk in hour_keys}
    for _rk, vals in cells.items():
        for hk, v in vals.items():
            if hk in out:
                out[hk] += v
    return out


def grand_total(cells: dict[str, dict[str, float]]) -> float:
    """Sum across all hourly cells, excluding cumulative (which is already a
    running total and would double-count)."""
    return sum(
        v
        for vals in cells.values()
        for hk, v in vals.items()
        if hk != CUMULATIVE_KEY
    )


def safe_pct(numer: float, denom: float) -> float:
    return (numer / denom * 100.0) if denom else 0.0
