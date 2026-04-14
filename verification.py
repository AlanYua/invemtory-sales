"""庫存/進貨/退貨/銷貨查核：客戶 vs 系統，支援 movement 加總驗證。"""
from __future__ import annotations

import pandas as pd


VERIFY_COLS = ["customer", "EAN", "Name", "store", "qty"]

# 新版查驗（統一上傳欄位）
VERIFY_V2_COLS = ["EAN", "品名", "類型", "數量"]
VERIFY_V2_TYPES = ["進貨", "退貨", "庫存"]


def _norm_str(s: object) -> str:
    return str(s).strip()


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


def _coerce_qty(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0)


def _pick_col(d: pd.DataFrame, want: list[str], alts: list[str]) -> str | None:
    cols = {str(c).strip(): c for c in d.columns}
    for w in want:
        if w in cols:
            return cols[w]
    for a in alts:
        if a in cols:
            return cols[a]
    return None


def load_verify_v2(df: pd.DataFrame, *, forced_type: str | None = None) -> pd.DataFrame:
    """
    新版查驗上傳格式：
      - EAN / 品名 / 類型 / 數量
    允許常見欄名變體；forced_type 會覆蓋類型欄（用於分檔上傳）。
    """
    d = _normalize_columns(df)
    c_ean = _pick_col(d, ["EAN"], ["ean", "條碼", "條碼號", "商品條碼"])
    c_name = _pick_col(d, ["品名"], ["Name", "商品", "品項", "商品名稱"])
    c_type = _pick_col(d, ["類型"], ["type", "Type", "種類", "類別", "單別"])
    c_qty = _pick_col(d, ["數量"], ["qty", "QTY", "數量(含)", "數量（含）", "數"])

    miss: list[str] = []
    if c_ean is None:
        miss.append("EAN")
    if c_name is None:
        miss.append("品名")
    if c_qty is None:
        miss.append("數量")
    if c_type is None and not forced_type:
        miss.append("類型")
    if miss:
        raise ValueError(f"缺少欄位: {miss}；目前欄位: {list(d.columns)}")

    out = pd.DataFrame(
        {
            "EAN": d[c_ean].astype(str).str.strip(),  # type: ignore[index]
            "品名": d[c_name].astype(str).str.strip(),  # type: ignore[index]
            "類型": (forced_type if forced_type else d[c_type].astype(str)).astype(str).str.strip(),  # type: ignore[index]
            "數量": _coerce_qty(d[c_qty]),  # type: ignore[index]
        }
    )
    out = out[(out["EAN"] != "") & (out["品名"] != "")]
    return out


def _normalize_verify_type(v: object) -> str:
    s = _norm_str(v)
    if not s:
        return s
    s2 = s.replace(" ", "")
    if "進" in s2:
        return "進貨"
    if "退" in s2:
        return "退貨"
    if "庫" in s2 or "存" in s2:
        return "庫存"
    return s


def aggregate_verify_v2(df: pd.DataFrame) -> pd.DataFrame:
    d = _normalize_columns(df)
    missing = [c for c in VERIFY_V2_COLS if c not in d.columns]
    if missing:
        raise ValueError(f"缺少欄位: {missing}；目前欄位: {list(d.columns)}")
    x = d[VERIFY_V2_COLS].copy()
    x["EAN"] = x["EAN"].astype(str).str.strip()
    x["品名"] = x["品名"].astype(str).str.strip()
    x["類型"] = x["類型"].map(_normalize_verify_type)
    x["數量"] = _coerce_qty(x["數量"])
    x = x[x["類型"].isin(VERIFY_V2_TYPES)]
    g = x.groupby(["EAN", "品名", "類型"], as_index=False)["數量"].sum()
    return g


def sales_cumulative_by_ean(
    sales_df: pd.DataFrame,
    *,
    customer: str,
    report_date_from: pd.Timestamp,
    report_date_to: pd.Timestamp,
) -> pd.DataFrame:
    """
    取「當月客戶累計銷售」：依 customer + report_date 區間篩選後，按 EAN 加總 qty。
    回傳欄位：EAN, sales_qty
    """
    if sales_df is None or len(sales_df) == 0:
        return pd.DataFrame(columns=["EAN", "sales_qty"])
    d = sales_df.copy()
    need = {"customer", "EAN", "qty", "report_date"}
    miss = sorted(list(need - set(d.columns)))
    if miss:
        raise ValueError(f"sales_df 缺少欄位: {miss}")
    rd = pd.to_datetime(d["report_date"], errors="coerce").dt.normalize()
    m = (d["customer"].astype(str).str.strip() == str(customer).strip()) & (
        rd >= pd.Timestamp(report_date_from).normalize()
    ) & (rd <= pd.Timestamp(report_date_to).normalize())
    x = d.loc[m, ["EAN", "qty"]].copy()
    if len(x) == 0:
        return pd.DataFrame(columns=["EAN", "sales_qty"])
    x["EAN"] = x["EAN"].astype(str).str.strip()
    x["qty"] = _coerce_qty(x["qty"])
    g = x.groupby("EAN", as_index=False)["qty"].sum().rename(columns={"qty": "sales_qty"})
    return g


def compute_verify_v2_report(
    *,
    system_df: pd.DataFrame,
    customer_df: pd.DataFrame,
    sales_df: pd.DataFrame | None,
    customer: str,
    report_date_from: pd.Timestamp,
    report_date_to: pd.Timestamp,
) -> pd.DataFrame:
    """
    報表顯示（值=QTY）：
      列：EAN/品名
      欄（固定輸出順序）：
        凌越(庫存) / 客戶(庫存) / 差異(庫存) /
        凌越(進貨) / 客戶(進貨) / 差異(進貨) /
        凌越(退貨) / 客戶(退貨) / 差異(退貨) /
        當月累計銷售

    差異 = 凌越 - 客戶
    當月累計銷售 = 依 customer + report_date 區間，按 EAN 加總 qty
    """
    s = aggregate_verify_v2(system_df).rename(columns={"數量": "qty_system"})
    c = aggregate_verify_v2(customer_df).rename(columns={"數量": "qty_customer"})
    key = ["EAN", "品名", "類型"]
    m = s.merge(c, on=key, how="outer")
    m["qty_system"] = _coerce_qty(m.get("qty_system"))
    m["qty_customer"] = _coerce_qty(m.get("qty_customer"))
    m["qty_diff"] = m["qty_system"] - m["qty_customer"]

    sales_g = (
        sales_cumulative_by_ean(
            sales_df if sales_df is not None else pd.DataFrame(),
            customer=customer,
            report_date_from=report_date_from,
            report_date_to=report_date_to,
        )
        if sales_df is not None
        else pd.DataFrame(columns=["EAN", "sales_qty"])
    )
    m = m.merge(sales_g, on=["EAN"], how="left")
    m["sales_qty"] = _coerce_qty(m.get("sales_qty"))
    out = m.pivot_table(
        index=["EAN", "品名"],
        columns="類型",
        values=["qty_system", "qty_customer", "qty_diff"],
        aggfunc="sum",
        fill_value=0,
    )
    rename0 = {
        "qty_system": "凌越",
        "qty_customer": "客戶",
        "qty_diff": "差異",
    }
    out.columns = [f"{rename0.get(a, str(a))}({b})" for a, b in out.columns.to_list()]
    out = out.reset_index()

    # 當月累計銷售：每個 EAN 一個值（不分 進/退/庫 類型）
    sales_out = sales_g[["EAN", "sales_qty"]].copy() if isinstance(sales_g, pd.DataFrame) else None
    if sales_out is None or len(sales_out) == 0:
        out["當月累計銷售"] = 0
    else:
        sales_out = sales_out.copy()
        sales_out["EAN"] = sales_out["EAN"].astype(str).str.strip()
        sales_out["sales_qty"] = _coerce_qty(sales_out["sales_qty"])
        out["EAN"] = out["EAN"].astype(str).str.strip()
        out = out.merge(sales_out, on="EAN", how="left")
        out["當月累計銷售"] = _coerce_qty(out.get("sales_qty"))
        out = out.drop(columns=["sales_qty"])

    # 欄位固定順序（缺欄補 0）
    want = [
        "EAN",
        "品名",
        "凌越(庫存)",
        "客戶(庫存)",
        "差異(庫存)",
        "凌越(進貨)",
        "客戶(進貨)",
        "差異(進貨)",
        "凌越(退貨)",
        "客戶(退貨)",
        "差異(退貨)",
        "當月累計銷售",
    ]
    for c in want:
        if c not in out.columns:
            out[c] = 0
    return out[want]


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
