"""
庫存查驗 + 銷售統計（Excel 上傳）
執行: streamlit run app.py
"""
from __future__ import annotations

import io

import pandas as pd
import streamlit as st

import persist_sales as ps
import sales_reports as sr
import verification as vf


st.set_page_config(page_title="庫存查驗 / 銷售統計", layout="wide")

st.title("庫存查驗 / 銷售統計")

tab_verify, tab_sales = st.tabs(["1. 查驗（客戶 vs 系統）", "2. 銷售統計"])

# --- 查驗 ---
with tab_verify:
    st.markdown(
        "上傳兩份 Excel，欄位需包含：**customer, EAN, Name, store, qty**（欄名需一致）。"
        "差異 = **系統 qty − 客戶 qty**。"
    )
    key_mode = st.radio(
        "對齊鍵",
        options=["full", "ean"],
        format_func=lambda x: "customer + EAN + Name + store"
        if x == "full"
        else "customer + EAN（跨店加總）",
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
            diff = vf.compute_diff(df_c, df_s, key_level=key_mode)
            st.subheader("差異明細（含兩邊皆為 0 的列）")
            st.dataframe(diff, use_container_width=True)
            only_diff = diff[diff["diff_system_minus_customer"] != 0]
            st.metric("筆數（僅差異≠0）", len(only_diff))
            st.dataframe(only_diff, use_container_width=True)
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as w:
                diff.to_excel(w, sheet_name="all", index=False)
                only_diff.to_excel(w, sheet_name="nonzero_diff", index=False)
            buf.seek(0)
            st.download_button(
                "下載差異 Excel",
                data=buf.getvalue(),
                file_name="verify_diff.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception as e:
            st.error(str(e))

# --- 銷售 ---
with tab_sales:
    st.markdown(
        "欄位：**Start_date, report_date, qty_kind, customer, brand, EAN, Name, store, qty**。"
        "**qty_kind** 含 `monthly` / `月` / `累積` 視為**累積上傳**：報表預設用 **本次累積 − 上次（同鍵）** 的區間量。"
    )
    st.caption(
        "銷售明細**只**透過 **Supabase** 持久化（無本機檔案）："
        f"{ps.persist_location_label()}。"
        " Streamlit Cloud 請在 **Secrets** 設定 `SUPABASE_URL`、`SUPABASE_SERVICE_ROLE_KEY`，"
        "並在 Supabase 執行 `supabase_schema.sql`。"
    )
    if not ps.supabase_configured():
        st.error(
            "尚未設定 Supabase：上傳／修正／baseline 覆寫都無法寫入雲端（重整後資料不見）。"
        )
    f_sales = st.file_uploader("銷售資料（可多次上傳合併）", type=["xlsx", "xls"], key="sales")

    if "sales_state_initialized" not in st.session_state:
        sdf, mbl, ldg, batches = ps.load_state()
        st.session_state.upload_batches = batches
        st.session_state.sales_df = sdf
        st.session_state.monthly_baseline = mbl
        st.session_state.last_monthly_debug = ldg
        st.session_state.sales_state_initialized = True

    if f_sales:
        try:
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
            st.success(
                f"已新增一批：{nm}（raw {len(raw)} 列）；總列數: {len(sdf)}；"
                f"baseline 鍵數: {len(mbl)}（已同步 Supabase）"
            )
        except Exception as e:
            st.error(str(e))

    with st.expander("上傳批次與修正（傳錯可移除或以新檔取代）", expanded=False):
        batches = st.session_state.upload_batches
        st.dataframe(ps.batch_summary_rows(batches), use_container_width=True)
        if not batches:
            st.caption("尚無批次。每成功上傳一個銷售 Excel 會建立一批（含舊版自動匯入的快照）。")
        else:
            labels = [
                f"{i + 1}. {b.get('filename', '')}  [{str(b.get('id', ''))[:8]}…]"
                for i, b in enumerate(batches)
            ]
            ix = st.selectbox("選擇要處理的批次", range(len(labels)), format_func=lambda j: labels[j])
            sel_id = batches[ix]["id"]
            sel_kind = batches[ix].get("kind") or "upload"
            fu_rep = st.file_uploader(
                "若要取代：選新銷售 Excel（欄位同主上傳）",
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
                    st.success("已移除並重算（已同步 Supabase）。")
                    st.rerun()
            with c_rp:
                if st.button("用新檔取代此批次", key="batch_replace"):
                    if sel_kind == "baseline_override":
                        st.warning("此為 baseline 批次：請先「移除」，再到下方 baseline 區上傳。")
                    elif fu_rep is None:
                        st.warning("請先選擇新檔案。")
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
                            st.success("已取代並重算（已同步 Supabase）。")
                            st.rerun()
                        except Exception as e:
                            st.error(str(e))

    df_all = st.session_state.sales_df

    with st.expander("Monthly baseline（匯出／覆蓋）", expanded=False):
        st.caption(
            "欄位："
            + ", ".join(sr.MONTHLY_BASELINE_KEYS + ["report_date", "qty_cumulative"])
        )
        bdf = st.session_state.monthly_baseline
        if len(bdf):
            st.dataframe(bdf, use_container_width=True)
        else:
            st.caption("目前無 baseline（尚未上傳 monthly）。")
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
            if up_bl:
                try:
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
                    st.success("已追加 baseline 覆寫批次（已同步 Supabase）。")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

    if len(df_all) == 0:
        st.info("請上傳至少一個銷售 Excel。")
    else:
        use_raw = st.radio(
            "報表 qty 口徑",
            options=[False, True],
            format_func=lambda x: "區間量（monthly 已扣上次累積）" if not x else "累積上傳原值（僅 monthly）",
            horizontal=True,
            key="pivot_qty_mode",
        )
        df_view = sr.dataframe_for_pivots(df_all, use_cumulative_raw=use_raw)

        if len(st.session_state.last_monthly_debug):
            with st.expander("上一批 monthly 扣減明細", expanded=False):
                st.dataframe(st.session_state.last_monthly_debug, use_container_width=True)

        dmin, dmax = df_all["report_date"].min(), df_all["report_date"].max()
        st.caption(f"report_date 範圍: {dmin.date()} ~ {dmax.date()}")

        st.subheader("報表 1：列＝週區間／品牌；欄＝customer；值＝qty")
        r1 = sr.report1_pivot(df_view)
        st.dataframe(r1, use_container_width=True)

        st.subheader("報表 2：時間篩選；列 brand／EAN／Name（全 0 品項已隱藏）；含合計")
        col_a, col_b = st.columns(2)
        with col_a:
            t0 = st.date_input("report_date 起", value=dmin.date(), key="r2s")
        with col_b:
            t1 = st.date_input("report_date 迄", value=dmax.date(), key="r2e")
        mask_df = sr.filter_by_report_date(
            df_view,
            pd.Timestamp(t0),
            pd.Timestamp(t1),
        )
        r2 = sr.report2_pivot(mask_df)
        st.dataframe(r2, use_container_width=True)
        st.caption("依品牌收折")
        for brand in sorted(mask_df["brand"].unique()):
            with st.expander(f"品牌: {brand}", expanded=False):
                sub = mask_df[mask_df["brand"] == brand]
                st.dataframe(sr.report2_pivot(sub), use_container_width=True)
        st.caption("依 customer 收折（欄為 store）")
        for cust in sorted(mask_df["customer"].unique()):
            with st.expander(f"Customer: {cust}", expanded=False):
                subc = mask_df[mask_df["customer"] == cust]
                st.dataframe(sr.report2_pivot_by_customer(subc), use_container_width=True)

        st.subheader("報表 3：Brand 篩選；列 EAN／Name；欄 customer（店舖已加總）")
        brands = sorted(df_view["brand"].unique())
        pick = st.multiselect("選品牌（可多選；不選＝全部）", brands, default=[])
        r3 = sr.report3_pivot(df_view, pick if pick else None)
        st.dataframe(r3, use_container_width=True)

        xl = sr.to_excel_bytes(
            {
                "report1": r1.reset_index(),
                "report2_filtered": sr.report2_pivot(mask_df).reset_index(),
                "report3": r3.reset_index(),
                "monthly_baseline": st.session_state.monthly_baseline,
                "last_upload_monthly_debug": st.session_state.last_monthly_debug,
            }
        )
        st.download_button(
            "下載三張報表（同一個 xlsx 多 sheet）",
            data=xl,
            file_name="sales_reports.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
