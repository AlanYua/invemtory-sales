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

# 跨月週與 monthly 對帳用（不含 Start_date；與月底 monthly 列加總對齊）
WEEKLY_RECONCILE_KEYS = ["customer", "brand", "EAN", "Name", "store"]


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


def _monthly_qty_sum_by_month(m: pd.DataFrame) -> dict[tuple, float]:
    """(customer, brand, EAN, Name, store, Period[M]) -> 該曆月內所有 monthly 列 qty 加總（已為增量）。"""
    if m is None or len(m) == 0:
        return {}
    out: dict[tuple, float] = {}
    for _, r in m.iterrows():
        ym = pd.Timestamp(r["report_date"]).to_period("M")
        k = tuple(str(r[c]) for c in WEEKLY_RECONCILE_KEYS) + (ym,)
        qv = pd.to_numeric(r["qty"], errors="coerce")
        out[k] = out.get(k, 0.0) + (0.0 if pd.isna(qv) else float(qv))
    return out


def _prior_weekly_sum_same_month_before(
    w_ref: pd.DataFrame,
    *,
    orig_ix: int,
    row_keys: tuple[str, ...],
    month_start: pd.Timestamp,
    month_end: pd.Timestamp,
    start_before: pd.Timestamp,
) -> float:
    """同鍵、report_date 落在 [month_start, month_end] 且 < start_before，排除原列 orig_ix。"""
    if len(w_ref) == 0:
        return 0.0
    rd = w_ref["report_date"].dt.normalize()
    msk = w_ref["_orig_ix"] != orig_ix
    msk &= rd >= pd.Timestamp(month_start).normalize()
    msk &= rd <= pd.Timestamp(month_end).normalize()
    msk &= rd < pd.Timestamp(start_before).normalize()
    for i, c in enumerate(WEEKLY_RECONCILE_KEYS):
        msk &= w_ref[c].astype(str) == row_keys[i]
    sub = w_ref.loc[msk, "qty"]
    return float(pd.to_numeric(sub, errors="coerce").fillna(0).sum())


def expand_weekly_cross_calendar_months(df: pd.DataFrame) -> pd.DataFrame:
    """
    將跨曆月的 weekly 列拆成多列，每列 Start_date~report_date 完全落在一個曆月內
    （例 4/28~5/3 → 4/28~4/30、5/1~5/3）。

    拆法（僅「恰好跨兩個曆月」時）：
    - 若該週「首曆月」內已有同鍵 monthly（如 4/30 上傳的 4/1~4/30 增量加總），且能對到資料：
      首段 qty = clip(該月 monthly qty 加總 − 同鍵下該月 report_date < 本週起點 的 weekly 加總, 0, 本列週 qty)；
      次段 qty = 本列週 qty − 首段（即 5/1~5/3 由 4/28~5/3 扣掉推算的 4/28~4/30）。
    - 否則：依各段曆日數占原區間比例分配（舊行為）。

    未跨月者原列不變。monthly 列原樣通過。
    拆段列會帶 parent_period＝原上傳區間字串；未拆者為 NaN。
    """
    if df is None or len(df) == 0:
        return df
    d = ensure_start_report_datetimes(df).dropna(subset=["Start_date", "report_date"])
    if len(d) == 0:
        return d
    is_m = d["qty_kind"].map(is_monthly_kind)
    m = d[is_m].copy()
    w = d[~is_m].copy()
    if "parent_period" not in m.columns and len(m):
        m["parent_period"] = pd.NA
    if len(w) == 0:
        return m if len(m) else d.iloc[0:0]

    monthly_by_ym = _monthly_qty_sum_by_month(m)
    w_ref = w.copy()
    w_ref["_orig_ix"] = w_ref.index.to_numpy()

    out_w: list[pd.Series] = []
    for _, row in w.iterrows():
        S = pd.Timestamp(row["Start_date"]).normalize()
        E = pd.Timestamp(row["report_date"]).normalize()
        q = pd.to_numeric(row["qty"], errors="coerce")
        q = 0.0 if pd.isna(q) else float(q)
        q_inc = row["qty_incremental"] if "qty_incremental" in row.index else None
        q_inc_num = pd.to_numeric(q_inc, errors="coerce")
        q_inc_f = float(q_inc_num) if pd.notna(q_inc_num) else None

        if pd.isna(S) or pd.isna(E) or S > E:
            nr = row.copy()
            nr["parent_period"] = pd.NA
            out_w.append(nr)
            continue

        if S.to_period("M") == E.to_period("M"):
            nr = row.copy()
            nr["parent_period"] = pd.NA
            out_w.append(nr)
            continue

        total_days = int((E - S).days) + 1
        if total_days <= 0:
            nr = row.copy()
            nr["parent_period"] = pd.NA
            out_w.append(nr)
            continue

        parent = f"{S:%Y-%m-%d}~{E:%Y-%m-%d}"
        prange = pd.period_range(S.to_period("M"), E.to_period("M"), freq="M")
        n_mon = len(prange)

        if n_mon == 2:
            m_first = S.to_period("M")
            m0 = m_first.to_timestamp().normalize()
            m1 = pd.Timestamp(m0 + pd.offsets.MonthEnd(0)).normalize()
            rk = tuple(str(row[c]) for c in WEEKLY_RECONCILE_KEYS)
            M_inc = monthly_by_ym.get(rk + (m_first,), 0.0)
            orig_ix = row.name
            prior = _prior_weekly_sum_same_month_before(
                w_ref,
                orig_ix=orig_ix,
                row_keys=rk,
                month_start=m0,
                month_end=m1,
                start_before=S,
            )
            if M_inc > 0:
                q_first = min(max(M_inc - prior, 0.0), q)
                q_second = q - q_first
                m_second = prange[1]
                m2_start = m_second.to_timestamp().normalize()
                m2_end = pd.Timestamp(m2_start + pd.offsets.MonthEnd(0)).normalize()
                segs = [
                    (max(S, m0), min(E, m1), q_first),
                    (max(S, m2_start), min(E, m2_end), q_second),
                ]
                for seg_s, seg_e, qseg in segs:
                    if seg_s > seg_e or qseg == 0.0:
                        continue
                    nr = row.copy()
                    nr["Start_date"] = seg_s
                    nr["report_date"] = seg_e
                    nr["qty"] = qseg
                    nr["parent_period"] = parent
                    if q_inc_f is not None and q > 0:
                        nr["qty_incremental"] = q_inc_f * (qseg / q)
                    elif q_inc_f is not None:
                        nr["qty_incremental"] = 0.0
                    out_w.append(nr)
                continue

        for per in prange:
            m0 = per.to_timestamp().normalize()
            m1 = pd.Timestamp(m0 + pd.offsets.MonthEnd(0)).normalize()
            seg_s = max(S, m0)
            seg_e = min(E, m1)
            if seg_s > seg_e:
                continue
            seg_days = int((seg_e - seg_s).days) + 1
            f = seg_days / float(total_days)
            nr = row.copy()
            nr["Start_date"] = seg_s
            nr["report_date"] = seg_e
            nr["qty"] = q * f
            nr["parent_period"] = parent
            if q_inc_f is not None:
                nr["qty_incremental"] = q_inc_f * f
            out_w.append(nr)

    w2 = pd.DataFrame(out_w) if out_w else w.iloc[0:0]
    if len(m) and len(w2):
        return pd.concat([m, w2], axis=0, ignore_index=True)
    if len(w2):
        return w2.reset_index(drop=True)
    return m.reset_index(drop=True) if len(m) else d.iloc[0:0]


def sales_df_for_calendar_month(
    df: pd.DataFrame,
    *,
    month_start: pd.Timestamp,
    month_end: pd.Timestamp,
) -> pd.DataFrame:
    """
    依「曆月」做銷售檢視用資料列（會 copy）：
    - monthly：只保留 report_date 落在 [month_start, month_end] 者，qty 不變。
    - weekly：先 expand_weekly_cross_calendar_months（跨兩曆月且有該月 monthly 時用對帳拆段），
      再只保留 report_date 落在 [month_start, month_end] 者。
    """
    if df is None or len(df) == 0:
        return df
    ms = pd.Timestamp(month_start).normalize()
    me = pd.Timestamp(month_end).normalize()
    d = ensure_start_report_datetimes(df).dropna(subset=["Start_date", "report_date"])
    if len(d) == 0:
        return d
    d = expand_weekly_cross_calendar_months(d)
    # expand 內 concat / 列組裝後 report_date 可能變 object；再轉一次避免 .dt AttributeError
    d = ensure_start_report_datetimes(d).dropna(subset=["Start_date", "report_date"])
    if len(d) == 0:
        return d.iloc[0:0]
    is_m = d["qty_kind"].map(is_monthly_kind)
    rd = d["report_date"].dt.normalize()
    sel = (rd >= ms) & (rd <= me)
    out = d.loc[sel].copy()
    if len(out) == 0:
        return d.iloc[0:0]
    return out.sort_values(
        ["report_date", "customer", "brand", "EAN", "store"],
        kind="mergesort",
    ).reset_index(drop=True)


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
    d = ensure_start_report_datetimes(d)

    # 報表 1 列「週區間」：
    # - weekly：以上傳的 Start_date~report_date 為週區間（例 4/1~4/27、4/28~5/3）；缺 Start_date 或起>迄時退回 ISO 週一~週日
    # - monthly（累積）：report_date 所屬曆月 1 號~月底
    rd = pd.to_datetime(d["report_date"], errors="coerce")
    rd_norm = rd.dt.normalize()
    sd_norm = pd.to_datetime(d["Start_date"], errors="coerce").dt.normalize()
    wk_start = rd_norm - pd.to_timedelta(rd_norm.dt.weekday, unit="D")
    wk_end = wk_start + pd.Timedelta(days=6)
    mo_period_start = rd_norm.dt.to_period("M").dt.to_timestamp()
    mo_period_end = pd.to_datetime(
        mo_period_start + pd.offsets.MonthEnd(0), errors="coerce"
    ).dt.normalize()
    is_m_row = d["qty_kind"].map(is_monthly_kind)
    period_iso = wk_start.dt.strftime("%Y-%m-%d") + "~" + wk_end.dt.strftime("%Y-%m-%d")
    bad_wk = sd_norm.isna() | (sd_norm > rd_norm)
    period_actual = sd_norm.dt.strftime("%Y-%m-%d") + "~" + rd_norm.dt.strftime("%Y-%m-%d")
    period_wk = period_iso.where(bad_wk, period_actual)
    period_mo = mo_period_start.dt.strftime("%Y-%m-%d") + "~" + mo_period_end.dt.strftime("%Y-%m-%d")
    d["_period"] = period_mo.where(is_m_row, period_wk)
    if "parent_period" in d.columns:
        pp = d["parent_period"]
        ps = pp.astype("string")
        has_parent = (~is_m_row) & pp.notna() & ps.str.strip().ne("")
        d.loc[has_parent, "_period"] = (
            d.loc[has_parent, "_period"].astype(str) + "（原：" + ps.loc[has_parent] + "）"
        )

    # weekly / monthly 分欄顯示
    d["_kind"] = d["qty_kind"].map(lambda x: "Monthly" if is_monthly_kind(x) else "Weekly")
    p = pd.pivot_table(
        d,
        index=["_period", "brand"],
        columns=["_kind", "customer"],
        values="qty",
        aggfunc="sum",
        fill_value=0,
    )
    p.index.names = ["週區間", "品牌"]
    out = _pivot_report1_period_subtotals(p)

    # 追加「整月加總」區塊：同樣拆品牌，再加一列總計
    try:
        if out is None or len(out) == 0:
            return out
        nlv = out.index.nlevels
        if nlv != 2:
            return out
        bottom_key = ("", MARGIN_ROW)
        if bottom_key not in out.index:
            return out

        rdc = pd.to_datetime(d["report_date"], errors="coerce").dropna()
        if len(rdc) == 0:
            return out
        # 以本次資料所屬月份為準（通常已先在 UI 依月份篩過）
        month_start = rdc.max().to_period("M").to_timestamp()
        month_end = (month_start + pd.offsets.MonthEnd(1)).normalize()
        month_label = f"{month_start:%Y-%m-%d}~{month_end:%Y-%m-%d}"

        sub = REPORT1_PERIOD_SUB
        # detail：排除每週小計與最底欄合計（只保留明細品牌列）
        is_detail = (
            (out.index.get_level_values(1).astype(str) != str(sub))
            & (out.index.get_level_values(1).astype(str) != str(MARGIN_ROW))
        )
        detail = out.loc[is_detail].copy()

        # 各品牌整月加總：對同品牌跨週加總
        brand_totals = detail.groupby(level=1).sum(numeric_only=True)
        brand_totals.index = pd.MultiIndex.from_tuples(
            [(month_label, str(b)) for b in brand_totals.index.tolist()],
            names=out.index.names,
        )

        # 整月總計：沿用底部欄合計（只加明細，不含週小計），但換成 month_label +（整月加總）
        bot = out.loc[bottom_key]
        month_total_row = pd.DataFrame(
            [bot.to_dict()],
            index=pd.MultiIndex.from_tuples([(month_label, "（整月加總）")], names=out.index.names),
        )

        # 插在原本 bottom 之前；bottom 仍保留（欄合計）
        wo_bottom = out.drop(index=[bottom_key])
        out = pd.concat([wo_bottom, brand_totals, month_total_row, out.loc[[bottom_key]]])
    except Exception:
        pass
    return out


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
