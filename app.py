from __future__ import annotations

import hashlib
import io
import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# 強制優先載入本專案同層模組，避免誤載 site-packages 的同名套件
_BASE_DIR = Path(__file__).resolve().parent
_base_dir_str = str(_BASE_DIR)
if _base_dir_str not in sys.path:
    sys.path.insert(0, _base_dir_str)

import persist_sales as ps
import sales_reports as sr
import verification as vf

ADMIN_USER = "admin"


def _upload_signature(f: object) -> str:
    fid = getattr(f, "file_id", None)
    if fid:
        return f"id:{fid}"
    name = (getattr(f, "name", None) or "").encode()
    gv = getattr(f, "getvalue", None)
    if callable(gv):
        data = gv()
    else:
        try:
            f.seek(0)
        except Exception:
            pass
        data = f.read()
        try:
            f.seek(0)
        except Exception:
            pass
    return "h:" + hashlib.sha256(name + data).hexdigest()


def _admin_password() -> str:
    p = os.getenv("ADMIN_PASSWORD", "").strip()
    if p:
        return p
    try:
        return str(st.secrets.get("ADMIN_PASSWORD", "")).strip()
    except Exception:
        return ""


def _pivot_for_display(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    if df is None or len(df) == 0:
        return df, {}

    def _label(col: object) -> str:
        if isinstance(col, tuple):
            parts = [str(x).strip() for x in col if str(x).strip() not in ("", "None", "<NA>")]
            if len(parts) >= 2 and parts[0] in {"Weekly", "Monthly"}:
                # 隱藏 Weekly/Monthly，但仍需讓欄位保持可區分（避免同 customer 變成重複欄名）
                # 用不可見字元做 disambiguation：畫面看起來一樣，但字串不同。
                invis = "\u200b" if parts[0] == "Weekly" else "\u200b\u200b"
                return f"{parts[1]}{invis}"
            return " — ".join(parts) if parts else "欄"
        return str(col)

    out = df.reset_index()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [_label(c) for c in out.columns]
    else:
        out.columns = [_label(c) for c in out.columns]

    seen: dict[str, int] = {}
    deduped: list[str] = []
    for c in out.columns:
        lab = str(c)
        if lab not in seen:
            seen[lab] = 0
            deduped.append(lab)
        else:
            seen[lab] += 1
            deduped.append(f"{lab} ({seen[lab]})")
    out.columns = deduped

    cfg: dict = {}
    for c in out.columns:
        if pd.api.types.is_numeric_dtype(out[c]):
            cfg[c] = st.column_config.NumberColumn(str(c), format="%,.0f")
    return out, cfg


def _style_numbers_pos_red_neg_green(
    df: pd.DataFrame,
) -> "pd.io.formats.style.Styler | pd.DataFrame":
    if df is None or len(df) == 0:
        return df
    try:
        num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        if not num_cols:
            return df

        def _cell(v: object) -> str:
            try:
                x = float(v)
            except Exception:
                return ""
            if x > 0:
                return "color: #ff4d4f;"  # red
            if x < 0:
                return "color: #52c41a;"  # green
            return ""

        return df.style.map(_cell, subset=num_cols)
    except Exception:
        return df


def _style_report1_week_subtotals(df: pd.DataFrame) -> "pd.io.formats.style.Styler | pd.DataFrame":
    """
    Streamlit 的 dataframe 有時會把「小計/合計」那列看起來像被淡化。
    這裡主動把週小計列做成更清楚的視覺樣式（避免「反灰像被 disabled」的觀感）。
    """
    if df is None or len(df) == 0:
        return df
    if "品牌" not in df.columns:
        return df
    try:
        base = _style_numbers_pos_red_neg_green(df)
        styler = base if hasattr(base, "apply") else df.style
        sub = getattr(sr, "REPORT1_PERIOD_SUB", "（週小計）")
        is_sub = df["品牌"].astype(str) == str(sub)
        is_month_total = df["品牌"].astype(str) == "（整月加總）"
        is_margin = df["品牌"].astype(str) == str(getattr(sr, "MARGIN_ROW", "欄合計"))
        flag = is_sub | is_month_total | is_margin

        def _row_style(row: pd.Series) -> list[str]:
            if not bool(flag.loc[row.name]):
                return [""] * len(row)
            # 深色系：讓小計「更亮更粗」，避免看起來反灰
            return ["font-weight: 700; color: #E6E6E6; background-color: rgba(255,255,255,0.06)"] * len(
                row
            )

        return styler.apply(_row_style, axis=1)
    except Exception:
        return df


st.set_page_config(page_title="庫存查驗 / 銷售統計", layout="wide")

if not st.session_state.get("auth_ok"):
    st.title("登入")
    cfg_pw = _admin_password()
    if not cfg_pw:
        st.error("未設定密碼。")
        st.stop()
    with st.form("login"):
        u = st.text_input("帳號", value=ADMIN_USER)
        pw = st.text_input("密碼", type="password")
        ok = st.form_submit_button("登入")
    if ok:
        if u.strip() == ADMIN_USER and pw == cfg_pw:
            st.session_state.auth_ok = True
            st.rerun()
        st.error("帳號或密碼錯誤")
    st.stop()

with st.sidebar:
    if st.button("登出"):
        st.session_state.auth_ok = False
        st.rerun()

st.title("庫存查驗 / 銷售統計")

tab_verify, tab_sales = st.tabs(["1. 查驗", "2. 銷售統計"])

with tab_verify:
    # 重新查驗：清掉查驗相關 widget/state，允許同檔案重新上傳並重跑
    if "verify_reset_seq" not in st.session_state:
        st.session_state.verify_reset_seq = 0
    if st.button("重新查驗（清除查驗上傳/選項）", key="btn_verify_reset"):
        for k in [
            "cust",
            "sys",
            "verify_in",
            "verify_ret",
            "verify_sales_override",
            "verify_sales_month_sel",
            "verify_sales_day",
            "_verify_prev_month_sel",
        ]:
            st.session_state.pop(k, None)
        st.session_state.verify_reset_seq += 1
        st.rerun()

    # 新版查驗：先選客戶（用來撈該客戶當月累計銷售）
    sdf = st.session_state.get("sales_df")
    cust_opts: list[str] = []
    months: list[str] = []
    if isinstance(sdf, pd.DataFrame) and len(sdf):
        if "customer" in sdf.columns:
            cust_opts = sorted(sdf["customer"].astype(str).str.strip().unique().tolist())
        if "report_date" in sdf.columns:
            _rd = pd.to_datetime(sdf["report_date"], errors="coerce").dropna()
            if len(_rd):
                months = sorted(_rd.dt.strftime("%Y/%m").unique().tolist())
    if not months:
        months = [pd.Timestamp.today().strftime("%Y/%m")]

    c0, c1 = st.columns([2, 1])
    with c0:
        customer_sel = st.selectbox(
            "查驗客戶（先選，因為要撈該客戶當月累計銷售）",
            options=cust_opts if cust_opts else ["（無入庫銷售資料：請先到「銷售統計」上傳）"],
            key="verify_customer_sel",
        )
        sales_ready = not customer_sel.startswith("（無入庫銷售資料")
        if not sales_ready:
            st.warning("尚無入庫銷售資料：請先到「銷售統計」上傳；銷售統計頁仍可正常使用。")
    with c1:
        month_sel = st.selectbox(
            "查驗月份（YYYY/MM）",
            options=months,
            index=len(months) - 1,
            key="verify_v2_month_sel",
        )

    month_start = pd.to_datetime(month_sel + "/01", errors="coerce").normalize()
    month_end = (month_start + pd.offsets.MonthEnd(1)).normalize()
    st.caption(f"當月累計銷售區間：{month_start:%Y-%m-%d}~{month_end:%Y-%m-%d}")

    st.subheader("上傳檔案（欄位統一：EAN／品名／類型／數量）")
    s1, s2 = st.columns(2)
    with s1:
        st.caption("凌越（系統）")
        sys_mix = st.file_uploader(
            "系統檔（可混合進/退/庫，用類型欄判斷）",
            type=["xlsx", "xls"],
            key=f"verify_sys_mix_{st.session_state.verify_reset_seq}",
        )
        sys_in = st.file_uploader(
            "系統進貨（分檔可不含類型欄）",
            type=["xlsx", "xls"],
            key=f"verify_sys_in_{st.session_state.verify_reset_seq}",
        )
        sys_ret = st.file_uploader(
            "系統退貨（分檔可不含類型欄）",
            type=["xlsx", "xls"],
            key=f"verify_sys_ret_{st.session_state.verify_reset_seq}",
        )
        sys_inv = st.file_uploader(
            "系統庫存（分檔可不含類型欄）",
            type=["xlsx", "xls"],
            key=f"verify_sys_inv_{st.session_state.verify_reset_seq}",
        )
    with s2:
        st.caption("客戶")
        cust_mix = st.file_uploader(
            "客戶檔（可混合進/退/庫，用類型欄判斷）",
            type=["xlsx", "xls"],
            key=f"verify_cust_mix_{st.session_state.verify_reset_seq}",
        )
        cust_in = st.file_uploader(
            "客戶進貨（分檔可不含類型欄）",
            type=["xlsx", "xls"],
            key=f"verify_cust_in_{st.session_state.verify_reset_seq}",
        )
        cust_ret = st.file_uploader(
            "客戶退貨（分檔可不含類型欄）",
            type=["xlsx", "xls"],
            key=f"verify_cust_ret_{st.session_state.verify_reset_seq}",
        )
        cust_inv = st.file_uploader(
            "客戶庫存（分檔可不含類型欄）",
            type=["xlsx", "xls"],
            key=f"verify_cust_inv_{st.session_state.verify_reset_seq}",
        )

    def _read_v2(fu: object | None, forced_type: str | None) -> pd.DataFrame:
        if fu is None:
            return pd.DataFrame(columns=vf.VERIFY_V2_COLS)
        raw = pd.read_excel(fu)
        return vf.load_verify_v2(raw, forced_type=forced_type)

    try:
        if not sales_ready:
            # 不要 st.stop()，避免整個 app（含銷售統計 tab）被中止渲染
            raise RuntimeError("請先到「銷售統計」上傳銷售資料後，再進行查驗。")
        sys_parts = [
            _read_v2(sys_mix, None),
            _read_v2(sys_in, "進貨"),
            _read_v2(sys_ret, "退貨"),
            _read_v2(sys_inv, "庫存"),
        ]
        cust_parts = [
            _read_v2(cust_mix, None),
            _read_v2(cust_in, "進貨"),
            _read_v2(cust_ret, "退貨"),
            _read_v2(cust_inv, "庫存"),
        ]
        sys_df_v2 = pd.concat([x for x in sys_parts if len(x)], ignore_index=True) if any(len(x) for x in sys_parts) else pd.DataFrame(columns=vf.VERIFY_V2_COLS)
        cust_df_v2 = pd.concat([x for x in cust_parts if len(x)], ignore_index=True) if any(len(x) for x in cust_parts) else pd.DataFrame(columns=vf.VERIFY_V2_COLS)

        if len(sys_df_v2) == 0 or len(cust_df_v2) == 0:
            st.info("請至少各上傳一份：系統檔與客戶檔（可用『混合檔』或分檔）。")
            raise RuntimeError("查驗檔案不足")

        rep = vf.compute_verify_v2_report(
            system_df=sys_df_v2,
            customer_df=cust_df_v2,
            sales_df=sdf if isinstance(sdf, pd.DataFrame) else None,
            customer=customer_sel,
            report_date_from=month_start,
            report_date_to=month_end,
        )

        st.subheader("查驗報表")
        st.dataframe(
            _style_numbers_pos_red_neg_green(rep),
            use_container_width=True,
            hide_index=True,
        )

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            rep.to_excel(w, sheet_name="verify_v2", index=False)
        buf.seek(0)
        st.download_button(
            "下載查驗報表 Excel",
            data=buf.getvalue(),
            file_name="verify_v2.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as e:
        st.error(str(e))

with tab_sales:
    if not ps.supabase_configured():
        st.error("未設定 Supabase。")
    f_sales = st.file_uploader("銷售資料（可多次上傳合併）", type=["xlsx", "xls"], key="sales")

    if "sales_state_initialized" not in st.session_state:
        sdf, mbl, ldg, batches = ps.load_state()
        st.session_state.upload_batches = batches
        st.session_state.sales_df = sdf
        st.session_state.monthly_baseline = mbl
        st.session_state.last_monthly_debug = ldg
        st.session_state.sales_state_initialized = True

    if not f_sales:
        st.session_state.pop("_sales_ingested_sig", None)
    else:
        u_sig = _upload_signature(f_sales)
        if st.session_state.get("_sales_ingested_sig") != u_sig:
            try:
                f_sales.seek(0)
                raw = pd.read_excel(f_sales)
                raw = sr.load_sales(raw)
                nm = getattr(f_sales, "name", "") or ""
                nb = ps.new_upload_batch(nm, raw)
                nxt = [*st.session_state.upload_batches, nb]
                sdf, mbl, ldg = ps.save_state(nxt)
                st.session_state.upload_batches = nxt
                st.session_state.sales_df = sdf
                st.session_state.monthly_baseline = mbl
                st.session_state.last_monthly_debug = ldg
                st.session_state["_sales_ingested_sig"] = u_sig
                st.success(f"已入庫：{nm}（{len(raw)} 列）")
            except Exception as e:
                st.error(str(e))

    with st.expander("上傳批次與修正", expanded=False):
        batches = st.session_state.upload_batches
        st.dataframe(ps.batch_summary_rows(batches), use_container_width=True)
        if batches:
            labels = [
                f"{i + 1}. {b.get('filename', '')}  [{str(b.get('id', ''))[:8]}…]"
                for i, b in enumerate(batches)
            ]
            ix = st.selectbox("選擇要處理的批次", range(len(labels)), format_func=lambda j: labels[j])
            sel_id = batches[ix]["id"]
            sel_kind = batches[ix].get("kind") or "upload"
            fu_rep = st.file_uploader(
                "取代用 Excel",
                type=["xlsx", "xls"],
                key="sales_replace_file",
            )
            c_rm, c_rp = st.columns(2)
            with c_rm:
                if st.button("移除此批次並重算", key="batch_remove"):
                    nxt = [b for b in batches if b["id"] != sel_id]
                    sdf, mbl, ldg = ps.save_state(nxt)
                    st.session_state.upload_batches = nxt
                    st.session_state.sales_df = sdf
                    st.session_state.monthly_baseline = mbl
                    st.session_state.last_monthly_debug = ldg
                    st.success("已移除。")
                    st.rerun()
            with c_rp:
                if st.button("用新檔取代此批次", key="batch_replace"):
                    if sel_kind == "baseline_override":
                        st.warning("baseline 批次請先移除。")
                    elif fu_rep is None:
                        st.warning("未選檔案。")
                    else:
                        try:
                            raw = sr.load_sales(pd.read_excel(fu_rep))
                            nb = ps.new_upload_batch(getattr(fu_rep, "name", "") or "", raw)
                            nb_list = list(batches)
                            nb_list[ix] = nb
                            sdf, mbl, ldg = ps.save_state(nb_list)
                            st.session_state.upload_batches = nb_list
                            st.session_state.sales_df = sdf
                            st.session_state.monthly_baseline = mbl
                            st.session_state.last_monthly_debug = ldg
                            st.success("已取代。")
                            st.rerun()
                        except Exception as e:
                            st.error(str(e))

    df_all = st.session_state.sales_df

    with st.expander("Monthly baseline", expanded=False):
        bdf = st.session_state.monthly_baseline
        if len(bdf):
            st.dataframe(bdf, use_container_width=True)
        b1, b2 = st.columns(2)
        with b1:
            st.download_button(
                "下載 baseline Excel",
                data=sr.to_excel_bytes({"monthly_baseline": bdf}),
                file_name="monthly_baseline.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_bl",
            )
        with b2:
            up_bl = st.file_uploader(
                "覆蓋 baseline（Excel）",
                type=["xlsx", "xls"],
                key="up_bl",
            )
            if not up_bl:
                st.session_state.pop("_bl_up_ingested_sig", None)
            else:
                bl_sig = _upload_signature(up_bl)
                if st.session_state.get("_bl_up_ingested_sig") != bl_sig:
                    try:
                        if hasattr(up_bl, "seek"):
                            up_bl.seek(0)
                        bl = sr.load_monthly_baseline(pd.read_excel(up_bl))
                        nxt = [
                            *st.session_state.upload_batches,
                            ps.new_baseline_override_batch(
                                getattr(up_bl, "name", "") or "", bl
                            ),
                        ]
                        sdf, mbl, ldg = ps.save_state(nxt)
                        st.session_state.upload_batches = nxt
                        st.session_state.sales_df = sdf
                        st.session_state.monthly_baseline = mbl
                        st.session_state.last_monthly_debug = ldg
                        st.session_state["_bl_up_ingested_sig"] = bl_sig
                        st.success("baseline 已更新。")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))

    if len(df_all) == 0:
        pass
    else:
        use_raw = st.radio(
            "qty 口徑",
            options=[False, True],
            format_func=lambda x: "區間量" if not x else "累積原值",
            horizontal=True,
            key="pivot_qty_mode",
        )
        df_view = sr.dataframe_for_pivots(df_all, use_cumulative_raw=use_raw)

        if len(st.session_state.last_monthly_debug):
            with st.expander("monthly 明細", expanded=False):
                st.dataframe(st.session_state.last_monthly_debug, use_container_width=True)

        qsum = float(df_view["qty"].sum())
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("總銷量 qty", f"{qsum:,.0f}")
        m2.metric("明細列數", f"{len(df_view):,}")
        m3.metric("品牌數", f"{df_view['brand'].nunique():,}")
        m4.metric("客戶數", f"{df_view['customer'].nunique():,}")

        st.subheader("報表查詢")
        dv = sr.ensure_start_report_datetimes(df_view)

        # 以 report_date 做「整月」查詢；另外提供單一銷售日僅用於顯示週別（不影響查詢範圍）
        rdc = (
            dv["report_date"].dropna()
            if len(dv) and "report_date" in dv.columns
            else pd.Series([], dtype="datetime64[ns]")
        )
        months: list[str] = []
        if len(rdc):
            months = sorted(pd.to_datetime(rdc, errors="coerce").dropna().dt.strftime("%Y/%m").unique().tolist())
        if not months:
            months = [pd.Timestamp.today().strftime("%Y/%m")]

        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            month_sel = st.selectbox(
                "銷售月份（YYYY/MM）",
                options=months,
                index=len(months) - 1,
                key="q_sales_month_sel",
            )

        month_start = pd.to_datetime(month_sel + "/01", errors="coerce").normalize()
        month_end = (month_start + pd.offsets.MonthEnd(1)).normalize()

        prev_month_sel = st.session_state.get("_q_prev_month_sel")
        if prev_month_sel != month_sel:
            st.session_state["q_sales_day"] = month_start.date()
            st.session_state["_q_prev_month_sel"] = month_sel
        else:
            cur_day = st.session_state.get("q_sales_day")
            if cur_day:
                _d = pd.Timestamp(cur_day).normalize()
                if _d < month_start or _d > month_end:
                    st.session_state["q_sales_day"] = month_start.date()

        with c2:
            sales_day = st.date_input(
                "銷售日（看屬於哪週）",
                value=month_start.date(),
                min_value=month_start.date(),
                max_value=month_end.date(),
                key="q_sales_day",
            )

        try:
            wk_s, wk_e = sr.week_range_monday_sunday(pd.Timestamp(sales_day))
        except Exception:
            wk_s, wk_e = month_start, month_start + pd.Timedelta(days=6)

        with c3:
            st.caption(
                f"查詢區間：{month_start:%Y-%m-%d}~{month_end:%Y-%m-%d}（整月，以 report_date 篩選）｜"
                f"所選銷售日週別：{wk_s:%Y-%m-%d}~{wk_e:%Y-%m-%d}"
            )

        df_base = sr.filter_start_report_dates(
            dv,
            report_date_from=month_start,
            report_date_to=month_end,
        )

        all_brands = sorted(df_view["brand"].unique())
        all_customers = sorted(df_view["customer"].unique())
        br12 = st.multiselect("品牌（1、2）", all_brands, default=[], key="br12")
        df_r12 = sr.filter_brands(df_base, br12 or None)
        br3 = st.multiselect("品牌（3）", all_brands, default=[], key="br3")
        cu3 = st.multiselect("Customer（3）", all_customers, default=[], key="cu3")
        df_r3 = sr.filter_customers(sr.filter_brands(df_base, br3 or None), cu3 or None)

        r1 = sr.report1_pivot(df_r12)
        r2 = sr.report2_pivot(df_r12)
        r3 = sr.report3_pivot(df_r3)

        tab_r1, tab_r2, tab_r3, tab_dl = st.tabs(
            ["報表 1", "報表 2", "報表 3", "匯出 xlsx"]
        )

        with tab_r1:
            d1, c1 = _pivot_for_display(r1)
            st.dataframe(
                _style_report1_week_subtotals(d1),
                use_container_width=True,
                column_config=c1,
                hide_index=True,
            )

        with tab_r2:
            d2, c2 = _pivot_for_display(r2)
            st.dataframe(
                _style_numbers_pos_red_neg_green(d2),
                use_container_width=True,
                column_config=c2,
                hide_index=True,
            )

        with tab_r3:
            d3, c3 = _pivot_for_display(r3)
            st.dataframe(
                _style_numbers_pos_red_neg_green(d3),
                use_container_width=True,
                column_config=c3,
                hide_index=True,
            )

        with tab_dl:
            xl = sr.to_excel_bytes(
                {
                    "report1": r1.reset_index(),
                    "report2": r2.reset_index(),
                    "report3": r3.reset_index(),
                    "monthly_baseline": st.session_state.monthly_baseline,
                    "last_upload_monthly_debug": st.session_state.last_monthly_debug,
                }
            )
            st.download_button(
                "下載報表 1～3（同一個 xlsx 多 sheet）",
                data=xl,
                file_name="sales_reports.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
