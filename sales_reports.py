"""銷售統計：讀取上傳資料並產生報表 1～3（列／欄合計、依合計排序）與 Excel 匯出。"""
from __future__ import annotations

import pandas as pd


SALES_COLS = [
    "Start_date",
    "report_date",
    "qty_kind",
    "customer",
    "brand",
    "EAN",
    "Name",
    "store",
    "qty",
]

# 累積銷售對齊鍵：同一區間起點 + 品項 + 店點，扣「上次上傳的累積 qty」
MONTHLY_BASELINE_KEYS = [
    "Start_date",
    "customer",
    "brand",
    "EAN",
    "Name",
    "store",
]


def is_monthly_kind(v: object) -> bool:
    s = str(v).strip().lower()
    return "month" in s or s in {"m", "月", "累積", "mtd", "cum"}


def _monthly_agg_keys() -> list[str]:
    return MONTHLY_BASELINE_KEYS + ["report_date"]


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


def load_sales(df: pd.DataFrame) -> pd.DataFrame:
    d = _normalize_columns(df)
    missing = [c for c in SALES_COLS if c not in d.columns]
    if missing:
        raise ValueError(f"缺少欄位: {missing}；目前欄位: {list(d.columns)}")
    d = d[SALES_COLS].copy()
    d["Start_date"] = pd.to_datetime(d["Start_date"], errors="coerce")
    d["report_date"] = pd.to_datetime(d["report_date"], errors="coerce")
    d["qty"] = pd.to_numeric(d["qty"], errors="coerce").fillna(0)
    for c in ["qty_kind", "customer", "brand", "EAN", "Name", "store"]:
        d[c] = d[c].astype(str).str.strip()
    d = d.dropna(subset=["Start_date", "report_date"])
    return d


def load_monthly_baseline(df: pd.DataFrame) -> pd.DataFrame:
    """從 Excel 還原 session baseline。"""
    d = _normalize_columns(df)
    need = MONTHLY_BASELINE_KEYS + ["report_date", "qty_cumulative"]
    miss = [c for c in need if c not in d.columns]
    if miss:
        raise ValueError(f"baseline 缺少欄位 {miss}；目前: {list(d.columns)}")
    d = d[need].copy()
    d["Start_date"] = pd.to_datetime(d["Start_date"], errors="coerce")
    d["report_date"] = pd.to_datetime(d["report_date"], errors="coerce")
    d["qty_cumulative"] = pd.to_numeric(d["qty_cumulative"], errors="coerce").fillna(0)
    for c in ["customer", "brand", "EAN", "Name", "store"]:
        d[c] = d[c].astype(str).str.strip()
    return d.dropna(subset=["Start_date", "report_date"])


def _baseline_rows_to_live(baseline: pd.DataFrame) -> dict[tuple, dict]:
    """每個 MONTHLY 鍵保留 report_date 最大的那筆累積。"""
    live: dict[tuple, dict] = {}
    if baseline is None or len(baseline) == 0:
        return live
    b = baseline.copy()
    for _, r in b.iterrows():
        k = tuple(r[x] for x in MONTHLY_BASELINE_KEYS)
        cur = live.get(k)
        rd = r["report_date"]
        qv = float(r["qty_cumulative"])
        if cur is None or rd >= cur["report_date"]:
            live[k] = {"report_date": rd, "qty_cumulative": qv}
    return live


def integrate_monthly_vs_baseline(
    chunk: pd.DataFrame,
    baseline: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    weekly（非 monthly）：qty 原樣；monthly：qty 改為「區間增量」＝本次累積上傳 − 上次（含 session baseline 與本檔較早列）。

    baseline 欄位：MONTHLY_BASELINE_KEYS + report_date + qty_cumulative
    """
    c = chunk.copy()
    empty_base = pd.DataFrame(columns=MONTHLY_BASELINE_KEYS + ["report_date", "qty_cumulative"])
    if len(c) == 0:
        return c.iloc[0:0], baseline.copy() if len(baseline) else empty_base, pd.DataFrame()

    is_m = c["qty_kind"].map(is_monthly_kind)
    w = c[~is_m].copy()
    m = c[is_m].copy()

    if len(m):
        gk = _monthly_agg_keys()
        agg_cols = {x: "first" for x in SALES_COLS if x not in gk and x != "qty"}
        agg_cols["qty"] = "sum"
        m = m.groupby(gk, as_index=False, dropna=False).agg(agg_cols)

    live = _baseline_rows_to_live(baseline if baseline is not None else empty_base)

    debug_rows: list[dict] = []
    m_out: list[pd.Series] = []

    if len(m):
        m = m.sort_values(
            list(MONTHLY_BASELINE_KEYS) + ["report_date"], kind="mergesort"
        )
        for _, row in m.iterrows():
            k = tuple(row[x] for x in MONTHLY_BASELINE_KEYS)
            raw = float(row["qty"])
            prev = live.get(k)
            note = ""
            if prev is None:
                inc = raw
                prev_rd = pd.NaT
                prev_q_disp = None
                note = "無上次累積：區間量＝本次累積上傳值"
            else:
                prev_rd = prev["report_date"]
                prev_q = prev["qty_cumulative"]
                prev_q_disp = prev_q
                if row["report_date"] < prev_rd:
                    inc = raw
                    note = "report_date 早於上次：不扣減（請檢查日期）"
                else:
                    inc = raw - prev_q

            new_row = row.copy()
            new_row["qty_cumulative_raw"] = raw
            new_row["qty_incremental"] = inc
            new_row["qty"] = inc
            m_out.append(new_row)

            debug_rows.append(
                {
                    **{x: row[x] for x in MONTHLY_BASELINE_KEYS},
                    "report_date": row["report_date"],
                    "qty_cumulative_raw": raw,
                    "prev_report_date": prev_rd,
                    "prev_qty_cumulative": prev_q_disp,
                    "qty_incremental": inc,
                    "note": note,
                }
            )

            if k not in live or row["report_date"] >= live[k]["report_date"]:
                live[k] = {"report_date": row["report_date"], "qty_cumulative": raw}

    w_out: list[pd.Series] = []
    for _, row in w.iterrows():
        nr = row.copy()
        nr["qty_cumulative_raw"] = float("nan")
        nr["qty_incremental"] = float(row["qty"])
        w_out.append(nr)

    out = pd.DataFrame(m_out + w_out)
    if len(out):
        out = out.sort_values(
            ["report_date", "customer", "brand", "EAN", "store"],
            kind="mergesort",
        ).reset_index(drop=True)

    new_base = pd.DataFrame(
        [{**dict(zip(MONTHLY_BASELINE_KEYS, kt)), **vv} for kt, vv in live.items()]
    )
    if len(new_base) == 0:
        new_base = empty_base.copy()

    debug_df = pd.DataFrame(debug_rows)
    return out, new_base, debug_df


def dataframe_for_pivots(df: pd.DataFrame, *, use_cumulative_raw: bool) -> pd.DataFrame:
    """報表用 qty：預設為扣過的增量；切換時 monthly 改用累積上傳 raw。"""
    if df is None or len(df) == 0:
        return df
    if not use_cumulative_raw:
        return df
    x = df.copy()
    if "qty_cumulative_raw" not in x.columns:
        return x
    m = x["qty_kind"].map(is_monthly_kind)
    x.loc[m, "qty"] = x.loc[m, "qty_cumulative_raw"].combine_first(x.loc[m, "qty"])
    return x


MARGINS_NAME = "合計"


def filter_start_report_dates(
    df: pd.DataFrame,
    *,
    start_date_from: pd.Timestamp | None = None,
    start_date_to: pd.Timestamp | None = None,
    report_date_from: pd.Timestamp | None = None,
    report_date_to: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """同時依 Start_date、report_date 區間篩選（依日期，含迄日當天）。"""
    if df is None or len(df) == 0:
        return df
    d = df
    sd = d["Start_date"].dt.normalize()
    rd = d["report_date"].dt.normalize()
    if start_date_from is not None:
        lo = pd.Timestamp(start_date_from).normalize()
        d = d[sd >= lo]
    if start_date_to is not None:
        hi = pd.Timestamp(start_date_to).normalize()
        d = d[sd <= hi]
    if report_date_from is not None:
        lo = pd.Timestamp(report_date_from).normalize()
        d = d[rd >= lo]
    if report_date_to is not None:
        hi = pd.Timestamp(report_date_to).normalize()
        d = d[rd <= hi]
    return d


def filter_brands(df: pd.DataFrame, brands: list[str] | None) -> pd.DataFrame:
    if not brands:
        return df
    return df[df["brand"].isin(brands)]


def filter_customers(df: pd.DataFrame, customers: list[str] | None) -> pd.DataFrame:
    if not customers:
        return df
    return df[df["customer"].isin(customers)]


def sort_and_margin_pivot(p: pd.DataFrame, *, margins_name: str = MARGINS_NAME) -> pd.DataFrame:
    """
    純資料 pivot（無 pandas margins）：列、欄依合計由高到低排序，再補列合計欄與欄合計列。
    """
    if p is None:
        return pd.DataFrame()
    if len(p) == 0 or len(p.columns) == 0:
        return p
    core = p.fillna(0)
    row_totals = core.sum(axis=1)
    col_totals = core.sum(axis=0)
    row_idx = row_totals.sort_values(ascending=False).index
    col_idx = col_totals.sort_values(ascending=False).index
    core = core.reindex(index=row_idx).reindex(columns=col_idx)
    out = core.copy()
    out[margins_name] = core.sum(axis=1)
    nlv = core.index.nlevels
    if nlv == 0:
        return out
    bottom_tuple = (margins_name,) if nlv == 1 else tuple("" for _ in range(nlv - 1)) + (margins_name,)
    bot: dict = {c: float(core[c].sum()) for c in core.columns}
    bot[margins_name] = float(out[margins_name].sum())
    bottom_df = pd.DataFrame(
        [bot],
        index=pd.MultiIndex.from_tuples([bottom_tuple], names=core.index.names),
    )
    return pd.concat([out, bottom_df])


def period_label(row: pd.Series) -> str:
    s = row["Start_date"]
    e = row["report_date"]
    return f"{s:%Y-%m-%d}~{e:%Y-%m-%d}"


def report1_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """列: 週區間 + brand；欄: customer；值: qty 加總；列／欄合計；列欄依合計由高到低。"""
    if len(df) == 0:
        return pd.DataFrame()
    d = df.copy()
    d["_period"] = d.apply(period_label, axis=1)
    p = pd.pivot_table(
        d,
        index=["_period", "brand"],
        columns="customer",
        values="qty",
        aggfunc="sum",
        fill_value=0,
    )
    p.index.names = ["週區間", "品牌"]
    return sort_and_margin_pivot(p)


def report2_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """列: brand, EAN, Name；欄: customer（跨 store 加總）；值: qty；列／欄合計；由高到低。"""
    if len(df) == 0:
        return pd.DataFrame()
    p = pd.pivot_table(
        df,
        index=["brand", "EAN", "Name"],
        columns="customer",
        values="qty",
        aggfunc="sum",
        fill_value=0,
    )
    return sort_and_margin_pivot(p)


def report3_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """
    列: brand, EAN, Name；值: qty。
    僅一個 customer 時欄為 store；多 customer 時欄為 customer + store（避免店名重複）。
    列／欄合計；由高到低。
    """
    if len(df) == 0:
        return pd.DataFrame()
    multi_cust = df["customer"].nunique() > 1
    cols: str | list[str] = ["customer", "store"] if multi_cust else "store"
    p = pd.pivot_table(
        df,
        index=["brand", "EAN", "Name"],
        columns=cols,
        values="qty",
        aggfunc="sum",
        fill_value=0,
    )
    return sort_and_margin_pivot(p)


def filter_by_report_date(
    df: pd.DataFrame,
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
) -> pd.DataFrame:
    d = df
    if start is not None:
        d = d[d["report_date"] >= start]
    if end is not None:
        d = d[d["report_date"] <= end]
    return d


def to_excel_bytes(
    sheets: dict[str, pd.DataFrame],
) -> bytes:
    import io

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for name, frame in sheets.items():
            safe = name[:31]
            frame.to_excel(w, sheet_name=safe)
    buf.seek(0)
    return buf.read()
