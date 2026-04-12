"""
庫存查驗 + 銷售統計（Excel 上傳）
執行: streamlit run app.py
"""
from __future__ import annotations

import io
import os

import pandas as pd
import streamlit as st

import persist_sales as ps
import sales_reports as sr
import verification as vf

ADMIN_USER = "admin"


def _admin_password() -> str:
    p = os.getenv("ADMIN_PASSWORD", "").strip()
    if p:
        return p
    try:
        return str(st.secrets.get("ADMIN_PASSWORD", "")).strip()
    except Exception:
        return ""


def _pivot_for_display(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Pivot 表：reset_index、扁平化欄名，並產生數字欄的千分位 column_config。"""
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

    # PyArrow 不允許重複欄名（扁平化 MultiIndex 或資料撞名）
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
        st.error(
            "尚未設定密碼：請在 **Streamlit Secrets**（或本機環境變數）加入 `ADMIN_PASSWORD = \"你的密碼\"`。"
        )
        st.stop()
    st.caption(f"帳號固定：**{ADMIN_USER}**（密碼由管理者設定，不寫在程式裡）。")
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
    st.caption(f"已登入：**{ADMIN_USER}**")
    if st.button("登出"):
        st.session_state.auth_ok = False
        st.rerun()

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

        qsum = float(df_view["qty"].sum())
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("總銷量 qty", f"{qsum:,.0f}")
        m2.metric("明細列數", f"{len(df_view):,}")
        m3.metric("品牌數", f"{df_view['brand'].nunique():,}")
        m4.metric("客戶數", f"{df_view['customer'].nunique():,}")

        st.subheader("報表查詢")
        st.caption(
            "依 **西曆月份（YYYY/MM）** 多選篩選：列若 **Start_date** 或 **report_date** 任一所屬月份命中即列入。"
            "三張報表皆套用。右欄為**列合計**、底列為**欄合計**；"
            "資料列／欄依合計由高到低（總列／欄固定在最後）。"
        )
        dv = sr.ensure_start_report_datetimes(df_view)
        month_opts: list[str] = []
        if len(dv):
            yms: set[str] = set()
            for _col in ("Start_date", "report_date"):
                s = dv[_col].dropna()
                if len(s):
                    yms.update(s.dt.strftime("%Y/%m").unique().tolist())
            month_opts = sorted(yms)
        if month_opts:
            sel_m = st.multiselect(
                "查詢月份（YYYY/MM；不選＝全部月份）",
                options=month_opts,
                default=[],
                key="q_months",
            )
            df_base = sr.filter_by_year_months(df_view, sel_m if sel_m else None)
        else:
            df_base = dv
        st.caption(f"篩選後明細列數：**{len(df_base):,}**（未篩選：{len(df_view):,}）")

        all_brands = sorted(df_view["brand"].unique())
        all_customers = sorted(df_view["customer"].unique())
        br12 = st.multiselect("品牌（報表 1、2；不選＝全部）", all_brands, default=[], key="br12")
        df_r12 = sr.filter_brands(df_base, br12 or None)
        br3 = st.multiselect("品牌（報表 3；不選＝全部）", all_brands, default=[], key="br3")
        cu3 = st.multiselect("Customer（報表 3；不選＝全部）", all_customers, default=[], key="cu3")
        df_r3 = sr.filter_customers(sr.filter_brands(df_base, br3 or None), cu3 or None)

        r1 = sr.report1_pivot(df_r12)
        r2 = sr.report2_pivot(df_r12)
        r3 = sr.report3_pivot(df_r3)

        tab_r1, tab_r2, tab_r3, tab_dl = st.tabs(
            ["報表 1", "報表 2", "報表 3", "匯出 xlsx"]
        )

        with tab_r1:
            st.caption("列＝週區間／Brand；欄＝ Customer；值＝ qty 加總；右欄「列合計」、底列「欄合計」。")
            d1, c1 = _pivot_for_display(r1)
            st.dataframe(d1, use_container_width=True, column_config=c1, hide_index=True)

        with tab_r2:
            st.caption("列＝ Brand／EAN／Name；欄＝ Customer（跨 store 加總）；值＝ qty。")
            d2, c2 = _pivot_for_display(r2)
            st.dataframe(d2, use_container_width=True, column_config=c2, hide_index=True)

        with tab_r3:
            st.caption(
                "列＝ Brand／EAN／Name；欄＝ Store（僅選一個或篩後只剩一客戶時）；"
                "若未選 Customer 或選多個客戶，欄為 **customer — store** 以免店名重複。"
            )
            d3, c3 = _pivot_for_display(r3)
            st.dataframe(d3, use_container_width=True, column_config=c3, hide_index=True)

        with tab_dl:
            st.caption("與上方「報表查詢」條件一致。")
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
