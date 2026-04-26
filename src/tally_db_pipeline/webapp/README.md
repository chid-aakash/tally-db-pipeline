# Webapp UI

FastAPI + Jinja2 server-rendered UI for the Tally Production Entry tool. All pages share a single styling system defined in `templates/base.html` — there is no build step, no JS framework, and no separate CSS files for templates (page-specific styles live in each template's `<style>` block).

## Stack

- **Server:** FastAPI app in `main.py` (~5k lines, all routes + view logic).
- **Templates:** Jinja2, in `templates/`. Every page extends `base.html`.
- **Static assets:** `static/` — logos, favicons, and standalone HTML pages (`app.html`, `policies.html`).
- **No bundler.** Plain CSS variables and vanilla JS inside templates. Hot-reload comes from `uvicorn --reload`.

## Run locally

```bash
uvicorn tally_db_pipeline.webapp.main:app --reload
```

Then open `http://localhost:8000/`.

## Layout

```
webapp/
├── main.py              # All FastAPI routes and view handlers
├── __init__.py
├── static/
│   ├── logos/           # AAPL + SEPL brand icons (animated favicon swap)
│   ├── app.html         # Standalone entry page (legacy/embedded)
│   └── policies.html
└── templates/
    ├── base.html              # Layout, navigation drawer, design tokens
    ├── home.html              # Landing
    ├── entry.html             # Tally voucher entry form
    ├── list.html              # Voucher entries list
    ├── review.html            # Voucher pre-post review
    ├── result.html            # Post-submit result
    ├── consumption.html       # Consumption report
    ├── print_summary.html     # Printable voucher summary
    ├── production_*.html      # Daily Production Report (DPR) flow
    ├── process_*.html         # Process config + catalog
    ├── shift_presets.html     # DPR shift slot presets
    ├── model_specs.html       # Model master + hole specs
    └── line_publish.html      # Line voucher publish to Tally
```

## Pages and routes

The drawer (hamburger) menu in `base.html` is the canonical nav. Top-level destinations:

| Drawer link            | Route                          | Template                     |
|------------------------|--------------------------------|------------------------------|
| New entry              | `/app`                         | `entry.html`                 |
| All entries            | `/entries`                     | `list.html`                  |
| Policies               | `/policies`                    | (static) `policies.html`     |
| Daily Production Report| `/production/daily-report`     | `production_list.html` etc.  |
| Production Review      | `/production/review`           | `production_review.html`     |
| Process Config         | `/production/processes`        | `process_config_index.html`  |
| Model Specs            | `/production/models`           | `model_specs.html`           |

DPR sub-routes (entry, edit, save, import, delete) all live under `/production/daily-report/{report_id}/...`. See `main.py` for the full list.

## Design system (defined in `base.html`)

**Brand palette** — navy + orange, on a soft off-white surface.

```css
--navy-900: #0f1e45;   /* header gradient start, h1 color */
--navy-800: #1e3a8a;   /* primary action, theme-color */
--navy-700: #2743a8;   /* focus ring, gradient mid */
--navy-50:  #eef2ff;
--orange-500: #f97316; /* shimmer, accent */
--orange-400: #fb923c; /* "Production" word in brand */
--orange-50:  #fff4e6;

--ink:        #111827; /* body text */
--ink-soft:   #374151; /* labels, table headers */
--muted:      #6b7280;
--line:       #e5e7eb;
--surface:    #ffffff; /* cards */
--surface-2:  #f7f8fc; /* page background */

--ok:   #16a34a;   --warn: #d97706;   --err: #dc2626;
--radius: 10px;
--shadow-sm: 0 1px 2px rgba(15,30,69,.06), 0 1px 3px rgba(15,30,69,.04);
--shadow-md: 0 6px 18px rgba(15,30,69,.08), 0 2px 6px rgba(15,30,69,.05);
--t-fast: 150ms cubic-bezier(.2,.7,.3,1);
--t-med:  280ms cubic-bezier(.2,.7,.3,1);
```

**Typography** — system font stack (`-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, ...`). H1 ≈ `1.5rem`, body ≈ `0.95rem`.

**Layout primitives**

- `header` — sticky, navy gradient, animated orange shimmer underline.
- `.bg-stage` + `.blob` — fixed soft animated background blobs (respects `prefers-reduced-motion`).
- `main` — centered, `max-width: 1400px`, page-in fade animation.
- `.card` — white, soft border + shadow, lifts on hover.
- `.drawer` — slide-in nav from left (320px wide, navy gradient).

**Components**

- Buttons: gradient navy by default; modifiers `.btn-secondary` (gray), `.btn-danger` (red), `.btn-success` (green). Hover lifts + brightens.
- Inputs: full-width, 8px radius, navy focus ring (`0 0 0 3px rgba(30,58,138,.15)`). Number inputs are right-aligned; `.qty-input` uses warm yellow background.
- Tables: zebra-free, hover row tint `rgba(30,58,138,0.04)`, `tr.has-qty` highlights filled rows in soft yellow.
- Status badges: `.status-draft`, `.status-submitted`, `.status-posted`, `.status-failed` — pill shape, semantic colors.

**Responsive**

- Tablet `≤1024px`: tighter padding.
- Phone `≤640px`: header stacks vertically; drawer narrows; buttons go full-width inside cards (but not inside `.flex-row`); inputs use `font-size: 16px` to prevent iOS zoom.

**Motion**

- Page-in fade, hover lifts, drawer slide, animated brand blobs, dual-logo flip (AAPL ⇄ SEPL every ~8s), favicon swap every 4s.
- Globally disabled when the OS reports `prefers-reduced-motion: reduce`.

## Conventions

- **Every page extends `base.html`** — drop content into `{% block content %}`. Don't add a second `<header>` or `<nav>`; the drawer is the nav.
- **Use design tokens.** Reach for `var(--navy-800)`, `var(--radius)`, etc. before hardcoding hex/px.
- **Page-specific CSS goes inside the template's own `<style>` block.** No shared CSS files yet; if a pattern repeats in 3+ templates, lift it into `base.html`.
- **Forms post to FastAPI handlers** in `main.py`. Most flows are server-rendered round-trips; small bits of vanilla JS handle drawer toggle, confirmations, and a few inline edits.
- **Brand assets** live in `static/logos/`. The header logo cross-fades between `aapl_icon.png` and `sepl_icon.png`.

## Adding a new page

1. Create `templates/your_page.html` and start it with `{% extends "base.html" %}`.
2. Put markup inside `{% block content %}...{% endblock %}` and wrap sections in `<div class="card">`.
3. Add a route in `main.py` returning `templates.TemplateResponse("your_page.html", {...})`.
4. If it's a top-level destination, add a `<a class="drawer-link" href="...">` entry in `base.html`'s `<aside id="drawer">`.
5. Reuse design tokens; only add page-local CSS for things genuinely unique to the page.

## Known UI debt / opportunities for a revamp

- `main.py` mixes routing and view logic; templates carry a lot of inline JS and styles.
- No shared CSS file — every page re-declares some local styles.
- Several DPR templates were added incrementally and would benefit from a unified table/list component.
- No automated visual or accessibility testing.
- Dark mode is not implemented (tokens are light-only).
