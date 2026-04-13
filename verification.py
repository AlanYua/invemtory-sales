"""庫存/進貨/退貨/銷貨查核：客戶 vs 系統，支援 movement 加總驗證。"""
from __future__ import annotations

import pandas as pd


VERIFY_COLS = ["customer", "EAN", "Name", "store", "qty"]


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


def _coerce_qty(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0)


def aggregate_lines(
    df: pd.DataFrame,
    *,
    key_level: str = "full",
) -> pd.DataFrame:
    """
    key_level:
      - 'full': customer+EAN+Name+store
      - 'ean': customer+EAN（Name/store 先加總，Name 取 first 僅供顯示）
    """
    d = _normalize_columns(df)
    missing = [c for c in VERIFY_COLS if c not in d.columns]
    if missing:
        raise ValueError(f"缺少欄位: {missing}；目前欄位: {list(d.columns)}")
    d = d[VERIFY_COLS].copy()
    d["qty"] = _coerce_qty(d["qty"])
    for c in ["customer", "EAN", "Name", "store"]:
        d[c] = d[c].astype(str).str.strip()
    if key_level == "ean":
        g = d.groupby(["customer", "EAN"], as_index=False).agg(
            Name=("Name", "first"),
            store=("store", lambda s: ",".join(sorted(set(s))) if len(set(s)) <= 3 else f"{len(set(s))} stores"),
            qty=("qty", "sum"),
        )
        return g
    g = d.groupby(["customer", "EAN", "Name", "store"], as_index=False)["qty"].sum()
    return g


def compute_diff(
    customer_df: pd.DataFrame,
    system_df: pd.DataFrame,
    *,
    key_level: str = "full",
) -> pd.DataFrame:
    c = aggregate_lines(customer_df, key_level=key_level).rename(
        columns={"qty": "qty_customer"}
    )
    s = aggregate_lines(system_df, key_level=key_level).rename(
        columns={"qty": "qty_system"}
    )
    key = list(c.columns)
    key.remove("qty_customer")
    m = s.merge(c, on=key, how="outer")
    m["qty_customer"] = m["qty_customer"].fillna(0)
    m["qty_system"] = m["qty_system"].fillna(0)
    m["diff_system_minus_customer"] = m["qty_system"] - m["qty_customer"]
    # 只保留有差異的列可選；這裡全留，方便核對
    return m.sort_values(key).reset_index(drop=True)


def _as_verify_lines(df: pd.DataFrame) -> pd.DataFrame:
    """把任意 dataframe 轉成 VERIFY_COLS 形狀（缺欄就報錯）。"""
    d = _normalize_columns(df)
    missing = [c for c in VERIFY_COLS if c not in d.columns]
    if missing:
        raise ValueError(f"缺少欄位: {missing}；目前欄位: {list(d.columns)}")
    return d[VERIFY_COLS].copy()


def sales_df_to_verify_lines(
    sales_df: pd.DataFrame,
    *,
    week_start: pd.Timestamp | None = None,
    week_end: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    從銷售統計入庫資料（sales_reports.load_sales 後的 sales_df）萃取成查核用欄位。
    若提供 week_start/week_end，僅取該週（Start_date==week_start 且 report_date==week_end）。
    """
    if sales_df is None or len(sales_df) == 0:
        return pd.DataFrame(columns=VERIFY_COLS)
    d = sales_df.copy()
    need = {"customer", "EAN", "Name", "store", "qty"}
    miss = sorted(list(need - set(d.columns)))
    if miss:
        raise ValueError(f"sales_df 缺少欄位: {miss}")
    if week_start is not None:
        d = d[pd.to_datetime(d["Start_date"], errors="coerce").dt.normalize() == pd.Timestamp(week_start).normalize()]
    if week_end is not None:
        d = d[pd.to_datetime(d["report_date"], errors="coerce").dt.normalize() == pd.Timestamp(week_end).normalize()]
    out = d[["customer", "EAN", "Name", "store", "qty"]].copy()
    out["qty"] = _coerce_qty(out["qty"])
    for c in ["customer", "EAN", "Name", "store"]:
        out[c] = out[c].astype(str).str.strip()
    return out


def compute_reconcile(
    *,
    customer_df: pd.DataFrame,
    system_df: pd.DataFrame,
    purchase_df: pd.DataFrame | None,
    return_df: pd.DataFrame | None,
    sales_lines_df: pd.DataFrame | None,
    key_level: str = "full",
) -> pd.DataFrame:
    """
    查核公式：
      系統 - 銷售 - 退貨 + 進貨 = 客戶報表

    其中 purchase/return/sales_lines 可為 None（視為 0）。
    """
    c = aggregate_lines(_as_verify_lines(customer_df), key_level=key_level).rename(
        columns={"qty": "qty_customer"}
    )
    s = aggregate_lines(_as_verify_lines(system_df), key_level=key_level).rename(
        columns={"qty": "qty_system"}
    )
    p0 = purchase_df if purchase_df is not None else pd.DataFrame(columns=VERIFY_COLS)
    r0 = return_df if return_df is not None else pd.DataFrame(columns=VERIFY_COLS)
    sl0 = sales_lines_df if sales_lines_df is not None else pd.DataFrame(columns=VERIFY_COLS)
    p = aggregate_lines(_as_verify_lines(p0), key_level=key_level).rename(
        columns={"qty": "qty_purchase"}
    )
    r = aggregate_lines(_as_verify_lines(r0), key_level=key_level).rename(
        columns={"qty": "qty_return"}
    )
    sl = aggregate_lines(_as_verify_lines(sl0), key_level=key_level).rename(
        columns={"qty": "qty_sales"}
    )

    key = [x for x in c.columns if x != "qty_customer"]
    m = (
        s.merge(c, on=key, how="outer")
        .merge(p, on=key, how="outer")
        .merge(r, on=key, how="outer")
        .merge(sl, on=key, how="outer")
    )
    for q in ["qty_customer", "qty_system", "qty_purchase", "qty_return", "qty_sales"]:
        if q not in m.columns:
            m[q] = 0
        m[q] = _coerce_qty(m[q])

    m["qty_calc"] = m["qty_system"] - m["qty_sales"] - m["qty_return"] + m["qty_purchase"]
    m["diff_calc_minus_customer"] = m["qty_calc"] - m["qty_customer"]
    m["diff_system_minus_customer"] = m["qty_system"] - m["qty_customer"]
    return m.sort_values(key).reset_index(drop=True)
