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
    key_mode = st.radio(
        "對齊鍵",
        options=["full", "ean"],
        format_func=lambda x: "完整" if x == "full" else "EAN",
        horizontal=True,
    )
    c1, c2 = st.columns(2)
    with c1:
        f_cust = st.file_uploader("客戶報表", type=["xlsx", "xls"], key="cust")
    with c2:
        f_sys = st.file_uploader("系統報表", type=["xlsx", "xls"], key="sys")

    if f_cust and f_sys:
        try:
            df_c = pd.read_excel(f_cust)
            df_s = pd.read_excel(f_sys)

            with st.expander("加總查核（系統-銷售-退貨+進貨＝客戶）", expanded=True):
                cc1, cc2, cc3 = st.columns(3)
                with cc1:
                    f_in = st.file_uploader("進貨（查核用）", type=["xlsx", "xls"], key="verify_in")
                with cc2:
                    f_ret = st.file_uploader("退貨（查核用）", type=["xlsx", "xls"], key="verify_ret")
                with cc3:
                    f_sales_override = st.file_uploader(
                        "銷貨（可選：若不上傳則用『銷售統計』入庫資料）",
                        type=["xlsx", "xls"],
                        key="verify_sales_override",
                    )

                # 銷貨來源：預設吃 sales_state（以 report_date 視為銷售日；用來挑查核區間）
                dr1, dr2 = st.columns(2)
                with dr1:
                    sales_from = st.date_input(
                        "銷售日起（= report_date）",
                        key="verify_sales_from",
                    )
                with dr2:
                    sales_to = st.date_input(
                        "銷售日迄（= report_date）",
                        key="verify_sales_to",
                    )
                rd_from = pd.Timestamp(sales_from)
                rd_to = pd.Timestamp(sales_to)
                if rd_from > rd_to:
                    rd_from, rd_to = rd_to, rd_from
                st.caption(f"銷貨區間：{rd_from:%Y-%m-%d}~{rd_to:%Y-%m-%d}")

                df_in = pd.read_excel(f_in) if f_in else None
                df_ret = pd.read_excel(f_ret) if f_ret else None

                if f_sales_override:
                    df_sales_lines = pd.read_excel(f_sales_override)
                else:
                    sdf = st.session_state.get("sales_df")
                    df_sales_lines = (
                        vf.sales_df_to_verify_lines(
                            sdf,
                            report_date_from=rd_from,
                            report_date_to=rd_to,
                        )
                        if isinstance(sdf, pd.DataFrame) and len(sdf)
                        else None
                    )
                    if df_sales_lines is None:
                        st.warning("尚無『銷售統計』入庫資料可用（或目前為空）。你也可以改用上傳銷貨檔。")

                recon = vf.compute_reconcile(
                    customer_df=df_c,
                    system_df=df_s,
                    purchase_df=df_in,
                    return_df=df_ret,
                    sales_lines_df=df_sales_lines,
                    key_level=key_mode,
                )

            st.subheader("差異明細")
            st.dataframe(recon, use_container_width=True)
            only_diff = recon[recon["diff_calc_minus_customer"] != 0]
            st.metric("加總差異≠0 筆數", len(only_diff))
            st.dataframe(only_diff, use_container_width=True)
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as w:
                recon.to_excel(w, sheet_name="all", index=False)
                only_diff.to_excel(w, sheet_name="nonzero_diff", index=False)
            buf.seek(0)
            st.download_button(
                "下載查核差異 Excel",
                data=buf.getvalue(),
                file_name="verify_reconcile.xlsx",
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

        # 單一銷售日 → 自動換算週別（週一~週日），用週區間去篩資料
        rdc = dv["report_date"].dropna() if len(dv) and "report_date" in dv.columns else pd.Series([], dtype="datetime64[ns]")
        default_day = (rdc.max().date() if len(rdc) else pd.Timestamp.today().date())
        sales_day = st.date_input("銷售日", value=default_day, key="q_sales_day")
        try:
            wk_s, wk_e = sr.week_range_monday_sunday(pd.Timestamp(sales_day))
            st.caption(f"週別：{wk_s:%Y-%m-%d}~{wk_e:%Y-%m-%d}")
            df_base = sr.filter_start_report_dates(
                dv,
                start_date_from=wk_s,
                start_date_to=wk_s,
                report_date_from=wk_e,
                report_date_to=wk_e,
            )
        except Exception as e:
            st.error(str(e))
            df_base = dv

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
            st.dataframe(d1, use_container_width=True, column_config=c1, hide_index=True)

        with tab_r2:
            d2, c2 = _pivot_for_display(r2)
            st.dataframe(d2, use_container_width=True, column_config=c2, hide_index=True)

        with tab_r3:
            d3, c3 = _pivot_for_display(r3)
            st.dataframe(d3, use_container_width=True, column_config=c3, hide_index=True)

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
