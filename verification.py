"""雙檔比對：客戶／條碼／門市／庫存／銷售。差異 = 檔案1 − 檔案2。"""
from __future__ import annotations

import pandas as pd

VERIFY_SIMPLE_COLS = ["客戶", "條碼", "門市", "庫存", "銷售"]


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


def _coerce_qty(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0)

def _to_int_series(s: pd.Series) -> pd.Series:
    """
    將數值欄轉成整數（nullable Int64），避免輸出/Excel 顯示 0.0000。
    若遇到非整數浮點（例如 1.5），會先四捨五入到最接近的整數。
    """
    x = pd.to_numeric(s, errors="coerce").fillna(0)
    x = x.round(0)
    return x.astype("Int64")


def _pick_col(d: pd.DataFrame, want: list[str], alts: list[str]) -> str | None:
    cols = {str(c).strip(): c for c in d.columns}
    for w in want:
        if w in cols:
            return cols[w]
    for a in alts:
        if a in cols:
            return cols[a]
    return None


def load_simple_inventory_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    欄位 客戶／條碼／門市／庫存／銷售（允許常見別名）。
    同一檔內若 (客戶,條碼,門市) 重複列會在 aggregate 時加總。
    """
    d = _normalize_columns(df)
    c_cu = _pick_col(d, ["客戶"], ["customer", "Customer", "客戶名稱", "客名", "客戶別"])
    c_bc = _pick_col(d, ["條碼"], ["EAN", "ean", "條碼號", "商品條碼"])
    c_st = _pick_col(d, ["門市"], ["store", "店鋪", "店舖", "店名", "門市名稱", "分店"])
    c_inv = _pick_col(d, ["庫存"], ["stock", "庫存量", "庫存數", "庫存數量"])
    c_sale = _pick_col(d, ["銷售"], ["sales", "銷售量", "銷貨", "銷售數量", "銷量"])
    miss: list[str] = []
    if c_cu is None:
        miss.append("客戶")
    if c_bc is None:
        miss.append("條碼")
    if c_st is None:
        miss.append("門市")
    if c_inv is None:
        miss.append("庫存")
    if c_sale is None:
        miss.append("銷售")
    if miss:
        raise ValueError(f"缺少欄位: {miss}；目前欄位: {list(d.columns)}")
    out = pd.DataFrame(
        {
            "客戶": d[c_cu].astype(str).str.strip(),  # type: ignore[index]
            "條碼": d[c_bc].astype(str).str.strip(),  # type: ignore[index]
            "門市": d[c_st].astype(str).str.strip(),  # type: ignore[index]
            "庫存": _coerce_qty(d[c_inv]),  # type: ignore[index]
            "銷售": _coerce_qty(d[c_sale]),  # type: ignore[index]
        }
    )
    out = out[(out["客戶"] != "") & (out["條碼"] != "") & (out["門市"] != "")]
    return out


def aggregate_simple_inventory_sales(df: pd.DataFrame) -> pd.DataFrame:
    d = _normalize_columns(df)
    missing = [c for c in VERIFY_SIMPLE_COLS if c not in d.columns]
    if missing:
        raise ValueError(f"缺少欄位: {missing}；目前欄位: {list(d.columns)}")
    x = d[VERIFY_SIMPLE_COLS].copy()
    x["客戶"] = x["客戶"].astype(str).str.strip()
    x["條碼"] = x["條碼"].astype(str).str.strip()
    x["門市"] = x["門市"].astype(str).str.strip()
    x["庫存"] = _coerce_qty(x["庫存"])
    x["銷售"] = _coerce_qty(x["銷售"])
    x = x[(x["客戶"] != "") & (x["條碼"] != "") & (x["門市"] != "")]
    return x.groupby(["客戶", "條碼", "門市"], as_index=False).agg(
        庫存=("庫存", "sum"), 銷售=("銷售", "sum")
    )


def compute_simple_diff_report(
    file1_df: pd.DataFrame,
    file2_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    以 (客戶, 條碼, 門市) 對齊；差異 = 檔案1 − 檔案2。
    輸出：客戶、條碼、門市、檔案1庫存、檔案2庫存、庫存差異、檔案1銷售、檔案2銷售、銷售差異
    """
    f1 = aggregate_simple_inventory_sales(file1_df).rename(
        columns={"庫存": "檔案1庫存", "銷售": "檔案1銷售"}
    )
    f2 = aggregate_simple_inventory_sales(file2_df).rename(
        columns={"庫存": "檔案2庫存", "銷售": "檔案2銷售"}
    )
    m = f1.merge(f2, on=["客戶", "條碼", "門市"], how="outer")
    for col in ["檔案1庫存", "檔案1銷售", "檔案2庫存", "檔案2銷售"]:
        if col not in m.columns:
            m[col] = 0
        m[col] = _coerce_qty(m[col])
    m["庫存差異"] = m["檔案1庫存"] - m["檔案2庫存"]
    m["銷售差異"] = m["檔案1銷售"] - m["檔案2銷售"]
    want = [
        "客戶",
        "條碼",
        "門市",
        "檔案1庫存",
        "檔案2庫存",
        "庫存差異",
        "檔案1銷售",
        "檔案2銷售",
        "銷售差異",
    ]
    out = m[want].sort_values(["客戶", "條碼", "門市"]).reset_index(drop=True)
    for c in ["檔案1庫存", "檔案2庫存", "庫存差異", "檔案1銷售", "檔案2銷售", "銷售差異"]:
        if c in out.columns:
            out[c] = _to_int_series(out[c])
    return out
