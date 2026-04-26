"""Microsoft Graph / OneDrive-for-Business client for the Model Specs sync.

Delegated OAuth (auth-code + refresh-token). Tokens persist to
``./data/ms_oauth_token.json`` so the interactive sign-in is one-time.

Reads the configured Excel table (``MS_TABLE_NAME``, default ``MASTER_DATA``)
out of the workbook at ``MS_WORKBOOK_PATH`` and returns the rows in the same
header-plus-data shape the existing xlsx upload path uses.

``MS_WORKBOOK_PATH`` may be either a SharePoint web-view URL (we resolve it
through the ``/shares/u!<b64>/driveItem`` endpoint) or a drive-relative path
like ``Documents/.../File.xlsx``.
"""
from __future__ import annotations

import base64
import json
import os
import secrets
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode

import requests


_TOKEN_PATH = Path("./data/ms_oauth_token.json")
_GRAPH = "https://graph.microsoft.com/v1.0"
_SCOPES = "Files.Read offline_access User.Read"


class OneDriveError(RuntimeError):
    pass


def _settings() -> dict[str, str]:
    s = {
        "client_id": (os.getenv("MS_CLIENT_ID") or "").strip(),
        "tenant_id": (os.getenv("MS_TENANT_ID") or "common").strip() or "common",
        "client_secret": (os.getenv("MS_CLIENT_SECRET") or "").strip(),
        "redirect_uri": (os.getenv("MS_REDIRECT_URI") or "http://localhost:8000/oauth/microsoft/callback").strip(),
        "workbook_path": (os.getenv("MS_WORKBOOK_PATH") or "").strip(),
        "table_name": (os.getenv("MS_TABLE_NAME") or "MASTER_DATA").strip() or "MASTER_DATA",
    }
    missing = [k for k in ("client_id", "tenant_id", "client_secret", "workbook_path") if not s[k]]
    if missing:
        raise OneDriveError(
            "missing env: " + ", ".join("MS_" + k.upper() for k in missing)
        )
    return s


def is_configured() -> bool:
    try:
        _settings()
        return True
    except OneDriveError:
        return False


def has_token() -> bool:
    return _TOKEN_PATH.exists()


# CSRF state for the OAuth round-trip — process-local is fine for a
# single-user local app. Survives a uvicorn reload only if the same worker
# handles both legs of the redirect; the user just retries if it doesn't.
_pending_states: set[str] = set()


def build_auth_url() -> str:
    s = _settings()
    state = secrets.token_urlsafe(24)
    _pending_states.add(state)
    params = {
        "client_id": s["client_id"],
        "response_type": "code",
        "redirect_uri": s["redirect_uri"],
        "response_mode": "query",
        "scope": _SCOPES,
        "state": state,
        "prompt": "select_account",
    }
    return (
        f"https://login.microsoftonline.com/{s['tenant_id']}"
        f"/oauth2/v2.0/authorize?{urlencode(params)}"
    )


def exchange_code(code: str, state: str) -> None:
    if state and state not in _pending_states:
        raise OneDriveError("OAuth state mismatch — start sign-in again")
    _pending_states.discard(state)
    s = _settings()
    resp = requests.post(
        f"https://login.microsoftonline.com/{s['tenant_id']}/oauth2/v2.0/token",
        data={
            "client_id": s["client_id"],
            "client_secret": s["client_secret"],
            "code": code,
            "redirect_uri": s["redirect_uri"],
            "grant_type": "authorization_code",
            "scope": _SCOPES,
        },
        timeout=30,
    )
    if resp.status_code != 200:
        raise OneDriveError(f"token exchange failed: {resp.status_code} {resp.text[:400]}")
    _save_token(resp.json())


def sign_out() -> None:
    if _TOKEN_PATH.exists():
        _TOKEN_PATH.unlink()


def _save_token(payload: dict[str, Any]) -> None:
    payload = dict(payload)
    payload["_obtained_at"] = int(time.time())
    _TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    _TOKEN_PATH.write_text(json.dumps(payload, indent=2))


def _load_token() -> dict[str, Any]:
    if not _TOKEN_PATH.exists():
        raise OneDriveError("not signed in — visit /oauth/microsoft/login first")
    return json.loads(_TOKEN_PATH.read_text())


def _refresh_if_needed(tok: dict[str, Any]) -> dict[str, Any]:
    expires_at = int(tok.get("_obtained_at", 0)) + int(tok.get("expires_in", 0))
    if time.time() < expires_at - 60:
        return tok
    s = _settings()
    refresh = tok.get("refresh_token")
    if not refresh:
        raise OneDriveError("session expired and no refresh token; sign in again")
    resp = requests.post(
        f"https://login.microsoftonline.com/{s['tenant_id']}/oauth2/v2.0/token",
        data={
            "client_id": s["client_id"],
            "client_secret": s["client_secret"],
            "refresh_token": refresh,
            "grant_type": "refresh_token",
            "scope": _SCOPES,
        },
        timeout=30,
    )
    if resp.status_code != 200:
        raise OneDriveError(f"refresh failed: {resp.status_code} {resp.text[:400]}")
    new_tok = resp.json()
    new_tok.setdefault("refresh_token", refresh)
    _save_token(new_tok)
    return _load_token()


def _access_token() -> str:
    return _refresh_if_needed(_load_token())["access_token"]


def _resolve_workbook(headers: dict[str, str]) -> tuple[str, str]:
    """Return (drive_id, item_id) for the configured workbook."""
    s = _settings()
    path = s["workbook_path"]
    if path.lower().startswith("http"):
        b64 = base64.urlsafe_b64encode(path.encode("utf-8")).rstrip(b"=").decode("ascii")
        url = f"{_GRAPH}/shares/u!{b64}/driveItem"
    else:
        url = f"{_GRAPH}/me/drive/root:/{quote(path.lstrip('/'))}"
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        raise OneDriveError(f"workbook lookup failed: {r.status_code} {r.text[:300]}")
    j = r.json()
    drive_id = (j.get("parentReference") or {}).get("driveId")
    item_id = j.get("id")
    if not (drive_id and item_id):
        raise OneDriveError(f"could not resolve workbook driveId/itemId from response: {j}")
    return drive_id, item_id


def default_worksheet_name() -> str:
    return (os.getenv("MS_WORKSHEET_NAME") or "MASTER_DATA").strip() or "MASTER_DATA"


def default_table_name() -> str:
    return (os.getenv("MS_TABLE_NAME") or "MASTER_DATA").strip() or "MASTER_DATA"


def fetch_master_table_rows(
    worksheet_name: str | None = None,
    table_name: str | None = None,
) -> list[list[str]]:
    """Fetch the table's full range as a header+data matrix.

    If ``worksheet_name`` is given, scope the lookup to that worksheet's table
    (`/workbook/worksheets('ws')/tables('tbl')/range`); otherwise look up the
    table at the workbook level. The latter works because Excel table names are
    workbook-unique.

    Returns rows in the same shape as the existing xlsx upload path, so the
    same parser (header-row detection, ``<num> MM`` hole columns) works
    unchanged."""
    _settings()  # validates env
    ws = (worksheet_name or "").strip() or None
    tbl = (table_name or "").strip() or default_table_name()
    headers = {"Authorization": f"Bearer {_access_token()}"}
    drive_id, item_id = _resolve_workbook(headers)
    if ws:
        url = (
            f"{_GRAPH}/drives/{drive_id}/items/{item_id}"
            f"/workbook/worksheets('{ws}')/tables('{tbl}')/range"
        )
    else:
        url = (
            f"{_GRAPH}/drives/{drive_id}/items/{item_id}"
            f"/workbook/tables('{tbl}')/range"
        )
    r = requests.get(url, headers=headers, timeout=60)
    if r.status_code != 200:
        raise OneDriveError(f"table fetch failed: {r.status_code} {r.text[:300]}")
    values = r.json().get("values") or []
    return [[("" if c is None else str(c)) for c in row] for row in values]
