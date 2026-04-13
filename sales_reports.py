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

# 上傳檔不再強制需要 Start_date；缺少時會依 report_date + qty_kind 推導。
UPLOAD_COLS = [
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
    missing = [c for c in UPLOAD_COLS if c not in d.columns]
    if missing:
        raise ValueError(f"缺少欄位: {missing}；目前欄位: {list(d.columns)}")

    # 先取必需欄位；Start_date 若有帶就先保留，後面會 normalize / 推導補齊
    keep = (["Start_date"] if "Start_date" in d.columns else []) + UPLOAD_COLS
    d = d[keep].copy()

    d["report_date"] = pd.to_datetime(d["report_date"], errors="coerce")
    d["qty"] = pd.to_numeric(d["qty"], errors="coerce").fillna(0)
    for c in ["qty_kind", "customer", "brand", "EAN", "Name", "store"]:
        d[c] = d[c].astype(str).str.strip()

    # Start_date 缺少就推導：
    # - weekly（非 monthly）：用 report_date 所屬週的週一
    # - monthly：用 report_date 所屬月份的 1 號（作為累積對齊鍵的區間起點）
    if "Start_date" in d.columns:
        d["Start_date"] = pd.to_datetime(d["Start_date"], errors="coerce")
    else:
        d["Start_date"] = pd.NaT
    is_m = d["qty_kind"].map(is_monthly_kind)
    rd_norm = d["report_date"].dt.normalize()
    wk_start = rd_norm - pd.to_timedelta(rd_norm.dt.weekday, unit="D")
    mo_start = rd_norm.dt.to_period("M").dt.to_timestamp()
    d.loc[d["Start_date"].isna() & ~is_m, "Start_date"] = wk_start
    d.loc[d["Start_date"].isna() & is_m, "Start_date"] = mo_start

    d = d[SALES_COLS].copy()
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
    df = ensure_start_report_datetimes(df)
    if not use_cumulative_raw:
        return df
    x = df.copy()
    if "qty_cumulative_raw" not in x.columns:
        return x
    m = x["qty_kind"].map(is_monthly_kind)
    x.loc[m, "qty"] = x.loc[m, "qty_cumulative_raw"].combine_first(x.loc[m, "qty"])
    return x


# 與資料內「合計」店名／客戶名區隔，避免 pivot 欄名重複
MARGIN_COL = "列合計"
MARGIN_ROW = "欄合計"
REPORT1_PERIOD_SUB = "（週小計）"


def ensure_start_report_datetimes(df: pd.DataFrame) -> pd.DataFrame:
    """Supabase／JSON 還原時日期常為字串，避免對非 datetime 用 .dt 觸發 AttributeError。"""
    if df is None or len(df) == 0:
        return df
    out = df.copy()
    if "Start_date" in out.columns:
        out["Start_date"] = pd.to_datetime(out["Start_date"], errors="coerce")
    if "report_date" in out.columns:
        out["report_date"] = pd.to_datetime(out["report_date"], errors="coerce")
    return out


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
    d = ensure_start_report_datetimes(df)
    d = d.dropna(subset=["Start_date", "report_date"])
    if len(d) == 0:
        return d
    sd = d["Start_date"].dt.normalize()
    rd = d["report_date"].dt.normalize()
    m = pd.Series(True, index=d.index)
    if start_date_from is not None:
        m &= sd >= pd.Timestamp(start_date_from).normalize()
    if start_date_to is not None:
        m &= sd <= pd.Timestamp(start_date_to).normalize()
    if report_date_from is not None:
        m &= rd >= pd.Timestamp(report_date_from).normalize()
    if report_date_to is not None:
        m &= rd <= pd.Timestamp(report_date_to).normalize()
    return d.loc[m]


def week_range_monday_sunday(sales_day: pd.Timestamp | str) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    依「單一銷售日」換算所屬週區間（週一~週日）。
    例：2026-04-10（五）→ 2026-04-06~2026-04-12
    """
    day = pd.to_datetime(sales_day, errors="coerce")
    if pd.isna(day):
        raise ValueError("sales_day 無法解析為日期")
    d = pd.Timestamp(day).normalize()
    start = d - pd.Timedelta(days=int(d.weekday()))  # Monday=0
    end = start + pd.Timedelta(days=6)
    return start, end


def filter_by_year_months(df: pd.DataFrame, months: list[str] | None) -> pd.DataFrame:
    """
    依西曆月份篩選。months 為 ['2026/04', ...] 格式；None 或 [] 表示不篩（全部）。
    列入規則：Start_date 或 report_date 所屬 YYYY/MM 任一落在選取月份即保留。
    """
    if df is None or len(df) == 0:
        return df
    if not months:
        return ensure_start_report_datetimes(df)
    d = ensure_start_report_datetimes(df).dropna(subset=["Start_date", "report_date"])
    if len(d) == 0:
        return d
    sel = set(months)
    ys = d["Start_date"].dt.strftime("%Y/%m")
    yr = d["report_date"].dt.strftime("%Y/%m")
    return d[ys.isin(sel) | yr.isin(sel)]


def filter_brands(df: pd.DataFrame, brands: list[str] | None) -> pd.DataFrame:
    if not brands:
        return df
    return df[df["brand"].isin(brands)]


def filter_customers(df: pd.DataFrame, customers: list[str] | None) -> pd.DataFrame:
    if not customers:
        return df
    return df[df["customer"].isin(customers)]


def sort_and_margin_pivot(p: pd.DataFrame, *, brand_first: bool = False) -> pd.DataFrame:
    """
    純資料 pivot（無 pandas margins）：欄依欄合計由高到低排序；
    列預設依列合計由高到低；brand_first=True 時先依第一層 index（品牌）的**品牌小計**
    （該品牌所有列合計加總）由高到低排品牌，同品牌內再依各列合計由高到低。
    再補右欄「列合計」與底列「欄合計」。
    """
    if p is None:
        return pd.DataFrame()
    if len(p) == 0 or len(p.columns) == 0:
        return p
    core = p.fillna(0)
    row_totals = core.sum(axis=1)
    col_totals = core.sum(axis=0)
    if brand_first and core.index.nlevels >= 1:
        tmp = row_totals.reset_index(name="__tot")
        brand_col = tmp.columns[0]
        brand_order = (
            tmp.groupby(brand_col, sort=False)["__tot"]
            .sum()
            .sort_values(ascending=False)
            .index
        )
        b_rank = {b: i for i, b in enumerate(brand_order)}
        tmp["__b_rank"] = tmp[brand_col].map(b_rank)
        tmp = tmp.sort_values(
            by=["__b_rank", "__tot"],
            ascending=[True, False],
            kind="mergesort",
        )
        row_idx = pd.MultiIndex.from_frame(tmp.drop(columns=["__tot", "__b_rank"]))
    else:
        row_idx = row_totals.sort_values(ascending=False).index
    col_idx = col_totals.sort_values(ascending=False).index
    core = core.reindex(index=row_idx).reindex(columns=col_idx)
    out = core.copy()
    out[MARGIN_COL] = core.sum(axis=1)
    nlv = core.index.nlevels
    if nlv == 0:
        return out
    bottom_tuple = (MARGIN_ROW,) if nlv == 1 else tuple("" for _ in range(nlv - 1)) + (MARGIN_ROW,)
    bot: dict = {c: float(core[c].sum()) for c in core.columns}
    bot[MARGIN_COL] = float(out[MARGIN_COL].sum())
    bottom_df = pd.DataFrame(
        [bot],
        index=pd.MultiIndex.from_tuples([bottom_tuple], names=core.index.names),
    )
    return pd.concat([out, bottom_df])


def period_label(row: pd.Series) -> str:
    e = row["report_date"]
    # monthly（累積）不應顯示成「1號~某日」的週區間；改成「截至日」的標籤
    if is_monthly_kind(row.get("qty_kind")):
        return f"{e:%Y-%m-%d}（月累積）"
    s = row["Start_date"]
    return f"{s:%Y-%m-%d}~{e:%Y-%m-%d}"


def _period_start_ts(period_label: object) -> pd.Timestamp:
    # 兼容：
    # - weekly: 'YYYY-MM-DD~YYYY-MM-DD'
    # - monthly: 'YYYY-MM-DD（月累積）'
    s = str(period_label).split("~", 1)[0].strip()
    # 把括號後綴去掉，避免 to_datetime 失敗
    if "（" in s:
        s = s.split("（", 1)[0].strip()
    t = pd.to_datetime(s, errors="coerce")
    return t if pd.notna(t) else pd.Timestamp.min


def _pivot_report1_period_subtotals(p: pd.DataFrame) -> pd.DataFrame:
    """
    報表 1：依週區間分塊；每區間內品牌依列合計高到低；區間末加「週小計」列（僅加該週各品牌）。
    底列欄合計只加總明細列（不含週小計），避免與週小計重複加總。
    """
    if p is None or len(p) == 0 or len(p.columns) == 0:
        return p if p is not None and len(p) else pd.DataFrame()
    core = p.fillna(0)
    lev0 = core.index.get_level_values(0).unique()
    periods = sorted(lev0, key=_period_start_ts)
    blocks: list[pd.DataFrame] = []
    sub = REPORT1_PERIOD_SUB
    for per in periods:
        m = core.index.get_level_values(0) == per
        blk = core.loc[m]
        if len(blk) == 0:
            continue
        rt = blk.sum(axis=1)
        blk_sorted = blk.reindex(index=rt.sort_values(ascending=False).index)
        blocks.append(blk_sorted)
        sub_s = blk.sum(axis=0)
        sub_row = pd.DataFrame(
            [sub_s.tolist()],
            columns=core.columns,
            index=pd.MultiIndex.from_tuples([(per, sub)], names=core.index.names),
        )
        blocks.append(sub_row)
    merged = pd.concat(blocks)
    is_detail = merged.index.get_level_values(1) != sub
    col_totals = merged.loc[is_detail].sum(axis=0)
    col_order = col_totals.sort_values(ascending=False).index
    merged = merged.reindex(columns=col_order)
    merged[MARGIN_COL] = merged.sum(axis=1)
    detail = merged.loc[is_detail]
    bot: dict[str | object, float] = {
        c: float(detail[c].sum()) for c in core.columns
    }
    bot[MARGIN_COL] = float(detail[MARGIN_COL].sum())
    nlv = merged.index.nlevels
    bottom_tuple = (MARGIN_ROW,) if nlv == 1 else tuple("" for _ in range(nlv - 1)) + (MARGIN_ROW,)
    bottom_df = pd.DataFrame(
        [bot],
        index=pd.MultiIndex.from_tuples([bottom_tuple], names=merged.index.names),
    )
    return pd.concat([merged, bottom_df])


def report1_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """列: 週區間／品牌，每週末列週小計；欄: customer；底列欄合計僅加明細。"""
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
    return _pivot_report1_period_subtotals(p)


def report2_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """列: brand, EAN, Name；欄: customer（跨 store 加總）；值: qty；品牌依小計高到低、同品牌內列合計高到低；欄依欄合計。"""
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
    return sort_and_margin_pivot(p, brand_first=True)


def report3_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """
    列: brand, EAN, Name；值: qty。
    僅一個 customer 時欄為 store；多 customer 時欄為 customer + store（避免店名重複）。
    品牌依小計高到低、同品牌內列合計高到低；欄依欄合計。
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
    return sort_and_margin_pivot(p, brand_first=True)


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
