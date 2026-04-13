"""庫存/進貨/退貨/銷貨查核：客戶 vs 系統，支援 movement 加總驗證。"""
from __future__ import annotations

import pandas as pd


VERIFY_COLS = ["customer", "EAN", "Name", "store", "qty"]
VERIFY_SIMPLE_COLS = ["EAN", "Name", "kind", "qty"]

# 新查驗上傳欄位（統一格式）
UPLOAD_SIMPLE_ALIASES: dict[str, list[str]] = {
    "EAN": ["EAN", "ean", "條碼", "條碼(EAN)", "商品條碼"],
    "Name": ["品名", "Name", "name", "商品名稱"],
    "kind": ["類型", "Type", "type", "項目", "分類"],
    "qty": ["數量", "QTY", "qty", "Qty", "數量(件)"],
    # 可選：若檔案有門市/客戶，可用來補值（沒有就用選到的客戶）
    "store": ["門市", "店點", "store", "Store"],
    "customer": ["客戶", "Customer", "customer"],
}

KIND_MAP: dict[str, str] = {
    "進貨": "進貨",
    "入庫": "進貨",
    "purchase": "進貨",
    "p": "進貨",
    "退貨": "退貨",
    "退回": "退貨",
    "return": "退貨",
    "r": "退貨",
    "庫存": "庫存",
    "stock": "庫存",
    "s": "庫存",
}


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


def _pick_col(d: pd.DataFrame, logical: str) -> str | None:
    cands = UPLOAD_SIMPLE_ALIASES.get(logical, [])
    cols = {str(c).strip(): c for c in d.columns}
    for cand in cands:
        if cand in cols:
            return cols[cand]
    return None


def normalize_simple_upload(
    df: pd.DataFrame,
    *,
    customer: str,
) -> pd.DataFrame:
    """
    新版查驗上傳檔：欄位統一為 EAN/品名/類型/數量（可選 門市/客戶）。
    - 沒有門市就以客戶補值（通常只進/退貨需要對門市；沒有就用客戶當門市名）
    - 類型會正規化成：進貨 / 退貨 / 庫存
    """
    d = _normalize_columns(df)
    c_ean = _pick_col(d, "EAN")
    c_name = _pick_col(d, "Name")
    c_kind = _pick_col(d, "kind")
    c_qty = _pick_col(d, "qty")
    missing: list[str] = []
    if not c_ean:
        missing.append("EAN")
    if not c_name:
        missing.append("品名")
    if not c_kind:
        missing.append("類型")
    if not c_qty:
        missing.append("數量")
    if missing:
        raise ValueError(f"缺少欄位: {missing}；目前欄位: {list(d.columns)}")

    out = pd.DataFrame(
        {
            "EAN": d[c_ean].astype(str).str.strip(),
            "Name": d[c_name].astype(str).str.strip(),
            "kind": d[c_kind].astype(str).str.strip(),
            "qty": _coerce_qty(d[c_qty]),
        }
    )
    # normalize kind
    kk = out["kind"].astype(str).str.strip()
    kk2 = kk.str.lower()
    out["kind"] = [
        KIND_MAP.get(v, KIND_MAP.get(v.lower(), v))  # type: ignore[arg-type]
        for v in kk2.tolist()
    ]
    out["kind"] = out["kind"].astype(str).str.strip()
    # drop empty keys
    out = out[(out["EAN"] != "") & (out["Name"] != "")].copy()
    # attach customer/store for compatibility with existing aggregators if needed
    out["customer"] = str(customer).strip()
    c_store = _pick_col(d, "store")
    if c_store:
        out["store"] = d[c_store].astype(str).str.strip().replace({"": None})
    else:
        out["store"] = None
    out["store"] = out["store"].fillna(out["customer"])
    return out[["customer", "store", "EAN", "Name", "kind", "qty"]].reset_index(drop=True)


def sales_df_to_monthly_sales(
    sales_df: pd.DataFrame,
    *,
    customer: str,
    report_date_from: pd.Timestamp,
    report_date_to: pd.Timestamp,
) -> pd.DataFrame:
    """從銷售統計入庫資料取出指定客戶、指定月份的當月累計銷售（依 EAN/Name 加總）。"""
    if sales_df is None or len(sales_df) == 0:
        return pd.DataFrame(columns=["EAN", "Name", "qty_sales"])
    d = sales_df.copy()
    need = {"customer", "EAN", "Name", "qty", "report_date"}
    miss = sorted(list(need - set(d.columns)))
    if miss:
        raise ValueError(f"sales_df 缺少欄位: {miss}")
    d["customer"] = d["customer"].astype(str).str.strip()
    d = d[d["customer"] == str(customer).strip()]
    rd = pd.to_datetime(d["report_date"], errors="coerce").dt.normalize()
    d = d[rd >= pd.Timestamp(report_date_from).normalize()]
    rd = rd.loc[d.index]
    d = d[rd <= pd.Timestamp(report_date_to).normalize()]
    if len(d) == 0:
        return pd.DataFrame(columns=["EAN", "Name", "qty_sales"])
    d["EAN"] = d["EAN"].astype(str).str.strip()
    d["Name"] = d["Name"].astype(str).str.strip()
    d["qty"] = _coerce_qty(d["qty"])
    g = d.groupby(["EAN", "Name"], as_index=False)["qty"].sum()
    return g.rename(columns={"qty": "qty_sales"})


def build_verify_report(
    *,
    system_upload: pd.DataFrame,
    customer_upload: pd.DataFrame,
    monthly_sales: pd.DataFrame,
) -> pd.DataFrame:
    """
    報表顯示（列）EAN/品名
    （欄）
      - 當月累計銷售
      - 系統(進貨/退貨/庫存)
      - 客戶(進貨/退貨/庫存)
      - 差異(進貨/退貨/庫存) = 系統 - 客戶
    """
    if system_upload is None:
        system_upload = pd.DataFrame()
    if customer_upload is None:
        customer_upload = pd.DataFrame()

    s = system_upload.copy()
    c = customer_upload.copy()
    for d in (s, c):
        if len(d) == 0:
            continue
        for col in ["EAN", "Name", "kind"]:
            d[col] = d[col].astype(str).str.strip()
        d["qty"] = _coerce_qty(d["qty"])

    kinds = ["進貨", "退貨", "庫存"]

    def _agg(x: pd.DataFrame, prefix: str) -> pd.DataFrame:
        if x is None or len(x) == 0:
            base = pd.DataFrame(columns=["EAN", "Name"])
            for k in kinds:
                base[f"{prefix}{k}"] = 0.0
            return base
        x = x[x["kind"].isin(kinds)].copy()
        p = pd.pivot_table(
            x,
            index=["EAN", "Name"],
            columns="kind",
            values="qty",
            aggfunc="sum",
            fill_value=0,
        )
        for k in kinds:
            if k not in p.columns:
                p[k] = 0
        p = p[kinds].copy()
        p.columns = [f"{prefix}{k}" for k in p.columns.tolist()]
        return p.reset_index()

    sys_agg = _agg(s, "系統-")
    cust_agg = _agg(c, "客戶-")

    ms = monthly_sales.copy() if monthly_sales is not None else pd.DataFrame(columns=["EAN", "Name", "qty_sales"])
    if len(ms):
        ms["EAN"] = ms["EAN"].astype(str).str.strip()
        ms["Name"] = ms["Name"].astype(str).str.strip()
        ms["qty_sales"] = _coerce_qty(ms["qty_sales"])

    out = sys_agg.merge(cust_agg, on=["EAN", "Name"], how="outer").merge(ms, on=["EAN", "Name"], how="outer")
    out = out.fillna(0)
    out = out.rename(columns={"qty_sales": "當月累計銷售"})
    for k in kinds:
        out[f"差異-{k}"] = out.get(f"系統-{k}", 0) - out.get(f"客戶-{k}", 0)

    col_order = (
        ["EAN", "Name", "當月累計銷售"]
        + [f"系統-{k}" for k in kinds]
        + [f"客戶-{k}" for k in kinds]
        + [f"差異-{k}" for k in kinds]
    )
    for cc in col_order:
        if cc not in out.columns:
            out[cc] = 0
    out = out[col_order].copy()
    return out.sort_values(["EAN", "Name"], kind="mergesort").reset_index(drop=True)


def sales_df_to_verify_lines(
    sales_df: pd.DataFrame,
    *,
    week_start: pd.Timestamp | None = None,
    week_end: pd.Timestamp | None = None,
    report_date_from: pd.Timestamp | None = None,
    report_date_to: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    從銷售統計入庫資料（sales_reports.load_sales 後的 sales_df）萃取成查核用欄位。
    篩選規則：
      - 若提供 week_start/week_end：僅取該週（Start_date==week_start 且 report_date==week_end）。
      - 若提供 report_date_from/to：以 report_date 視為銷售日，取區間（含起訖日）。
    """
    if sales_df is None or len(sales_df) == 0:
        return pd.DataFrame(columns=VERIFY_COLS)
    d = sales_df.copy()
    need = {"customer", "EAN", "Name", "store", "qty"}
    miss = sorted(list(need - set(d.columns)))
    if miss:
        raise ValueError(f"sales_df 缺少欄位: {miss}")
    sd = pd.to_datetime(d.get("Start_date"), errors="coerce").dt.normalize()
    rd = pd.to_datetime(d.get("report_date"), errors="coerce").dt.normalize()

    # 舊介面：指定某一週（Start_date/ report_date 成對）
    if week_start is not None:
        d = d[sd == pd.Timestamp(week_start).normalize()]
        rd = rd.loc[d.index]
    if week_end is not None:
        d = d[rd == pd.Timestamp(week_end).normalize()]
        sd = sd.loc[d.index]
        rd = rd.loc[d.index]

    # 新介面：以 report_date 當銷售日做區間查詢
    if report_date_from is not None:
        d = d[rd >= pd.Timestamp(report_date_from).normalize()]
        sd = sd.loc[d.index]
        rd = rd.loc[d.index]
    if report_date_to is not None:
        d = d[rd <= pd.Timestamp(report_date_to).normalize()]
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
