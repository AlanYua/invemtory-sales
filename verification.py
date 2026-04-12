"""庫存/進貨/退貨查驗：客戶 vs 系統 Excel 差異 (系統 - 客戶)。"""
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
