"""銷售狀態：依上傳批次重播；僅寫入 Supabase（Streamlit Cloud）。"""
from __future__ import annotations

import base64
import json
import pickle
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any

import pandas as pd

import sales_reports as sr

APP_STATE_ROW_ID = "default"


def _empty_baseline() -> pd.DataFrame:
    return pd.DataFrame(columns=sr.MONTHLY_BASELINE_KEYS + ["report_date", "qty_cumulative"])


def supabase_configured() -> bool:
    u, k = _get_supabase()
    return bool(u and k)


def persist_location_label() -> str:
    if supabase_configured():
        u, _ = _get_supabase()
        return f"Supabase（{u}）"
    return "未設定 Supabase（不會持久化；重整後資料不見）"


def _get_supabase() -> tuple[str | None, str | None]:
    import os

    u = os.getenv("SUPABASE_URL")
    k = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if u and k:
        return str(u).rstrip("/"), str(k)
    try:
        import streamlit as st

        if hasattr(st, "secrets"):
            u2 = st.secrets.get("SUPABASE_URL")
            k2 = st.secrets.get("SUPABASE_SERVICE_ROLE_KEY")
            if u2 and k2:
                return str(u2).rstrip("/"), str(k2)
    except Exception:
        pass
    return None, None


def _rest_headers(key: str) -> dict[str, str]:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _cloud_fetch_rows(url: str, key: str) -> list[dict[str, Any]]:
    endpoint = f"{url}/rest/v1/app_state?id=eq.{APP_STATE_ROW_ID}&select=payload"
    req = urllib.request.Request(endpoint, headers=_rest_headers(key), method="GET")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code in (404, 406):
            return []
        raise


def _cloud_load_blob() -> dict[str, Any] | None:
    url, key = _get_supabase()
    if not url or not key:
        return None
    rows = _cloud_fetch_rows(url, key)
    if not rows:
        return None
    b64 = rows[0].get("payload")
    if not b64:
        return None
    return pickle.loads(base64.b64decode(b64.encode("ascii")))


def _cloud_write_blob(blob: dict[str, Any]) -> None:
    url, key = _get_supabase()
    if not url or not key:
        raise RuntimeError("Supabase 未設定")
    raw = pickle.dumps(blob, protocol=4)
    b64 = base64.b64encode(raw).decode("ascii")
    hdrs = _rest_headers(key)
    body = json.dumps([{"id": APP_STATE_ROW_ID, "payload": b64}]).encode("utf-8")
    ins = f"{url}/rest/v1/app_state"
    req = urllib.request.Request(
        ins,
        data=body,
        headers={
            **hdrs,
            "Prefer": "resolution=merge-duplicates,return=minimal",
        },
        method="POST",
    )
    urllib.request.urlopen(req, timeout=120)


def replay_from_batches(batches: list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """依序重播批次 → sales_df、monthly_baseline、最後一次 monthly debug。"""
    baseline = _empty_baseline()
    parts: list[pd.DataFrame] = []
    last_dbg = pd.DataFrame()
    for b in batches:
        kind = b.get("kind") or "upload"
        if kind == "snapshot":
            s = b.get("sales_df")
            bl = b.get("monthly_baseline")
            if isinstance(s, pd.DataFrame) and len(s):
                parts.append(s.copy())
            if isinstance(bl, pd.DataFrame) and len(bl.columns):
                baseline = bl.copy()
            continue
        if kind == "baseline_override":
            bl = b.get("monthly_baseline")
            if isinstance(bl, pd.DataFrame) and len(bl.columns):
                baseline = bl.copy()
            continue
        raw = b.get("raw")
        if not isinstance(raw, pd.DataFrame) or len(raw) == 0:
            continue
        out, baseline, dbg = sr.integrate_monthly_vs_baseline(raw, baseline)
        parts.append(out)
        last_dbg = dbg
    if not parts:
        return pd.DataFrame(), baseline, last_dbg
    sales_df = pd.concat(parts, ignore_index=True)
    return sales_df, baseline, last_dbg


def _migrate_old_blob(blob: dict[str, Any]) -> list[dict[str, Any]]:
    """舊版只有 sales_df / baseline，轉成單一 snapshot 批次。"""
    if blob.get("upload_batches"):
        return blob["upload_batches"]
    sales_df = blob.get("sales_df")
    baseline = blob.get("monthly_baseline")
    if not isinstance(sales_df, pd.DataFrame):
        sales_df = pd.DataFrame()
    if not isinstance(baseline, pd.DataFrame):
        baseline = _empty_baseline()
    if len(sales_df) == 0 and (len(baseline) == 0 or len(baseline.columns) == 0):
        return []
    return [
        {
            "id": "legacy-snapshot",
            "filename": "（舊版資料，可整批取代或保留）",
            "uploaded_at": "",
            "kind": "snapshot",
            "sales_df": sales_df.copy(),
            "monthly_baseline": baseline.copy()
            if len(baseline.columns)
            else _empty_baseline(),
        }
    ]


def load_state() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    if not supabase_configured():
        return pd.DataFrame(), _empty_baseline(), pd.DataFrame(), []
    blob: dict[str, Any] | None = None
    try:
        blob = _cloud_load_blob()
    except Exception:
        blob = None
    if blob is None:
        return pd.DataFrame(), _empty_baseline(), pd.DataFrame(), []
    batches = _migrate_old_blob(blob)
    sales_df, baseline, dbg = replay_from_batches(batches)
    return sales_df, baseline, dbg, batches


def save_state(
    upload_batches: list[dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """寫入 Supabase；回傳重播後的 (sales_df, baseline, last_dbg)。"""
    if not supabase_configured():
        raise RuntimeError(
            "未設定 Supabase：請在 Streamlit Secrets（或環境變數）設定 "
            "`SUPABASE_URL` 與 `SUPABASE_SERVICE_ROLE_KEY`，並在資料庫執行 `supabase_schema.sql`。"
        )
    sales_df, baseline, dbg = replay_from_batches(upload_batches)
    out_blob = {
        "upload_batches": upload_batches,
        "last_monthly_debug": dbg,
    }
    _cloud_write_blob(out_blob)
    return sales_df, baseline, dbg


def new_upload_batch(filename: str, raw: pd.DataFrame) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "filename": filename or "(未命名)",
        "uploaded_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "kind": "upload",
        "raw": raw.copy(),
    }


def new_baseline_override_batch(filename: str, monthly_baseline: pd.DataFrame) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "filename": filename or "(baseline)",
        "uploaded_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "kind": "baseline_override",
        "monthly_baseline": monthly_baseline.copy(),
    }


def batch_summary_rows(batches: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for i, b in enumerate(batches, start=1):
        kind = b.get("kind") or "upload"
        fn = b.get("filename", "")
        bid = b.get("id", "")
        if kind == "snapshot":
            sdf = b.get("sales_df")
            n = len(sdf) if isinstance(sdf, pd.DataFrame) else 0
            rows.append(
                {
                    "序": i,
                    "檔名": fn,
                    "類型": "快照",
                    "列數": n,
                    "batch_id": bid,
                    "上傳時間": b.get("uploaded_at", ""),
                }
            )
        elif kind == "baseline_override":
            bl = b.get("monthly_baseline")
            n = len(bl) if isinstance(bl, pd.DataFrame) else 0
            rows.append(
                {
                    "序": i,
                    "檔名": fn,
                    "類型": "baseline 覆寫",
                    "列數": n,
                    "batch_id": bid,
                    "上傳時間": b.get("uploaded_at", ""),
                }
            )
        else:
            raw = b.get("raw")
            n = len(raw) if isinstance(raw, pd.DataFrame) else 0
            rows.append(
                {
                    "序": i,
                    "檔名": fn,
                    "類型": "上傳",
                    "列數": n,
                    "batch_id": bid,
                    "上傳時間": b.get("uploaded_at", ""),
                }
            )
    return pd.DataFrame(rows)
