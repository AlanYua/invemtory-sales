from __future__ import annotations

import io
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

_BASE_DIR = Path(__file__).resolve().parent
_base_dir_str = str(_BASE_DIR)
if _base_dir_str not in sys.path:
    sys.path.insert(0, _base_dir_str)

import verification as vf


def _style_numbers_pos_red_neg_green(
    df: pd.DataFrame,
) -> "pd.io.formats.style.Styler | pd.DataFrame":
    if df is None or len(df) == 0:
        return df
    try:
        diff_cols = [c for c in ("庫存差異", "銷售差異") if c in df.columns]
        if not diff_cols:
            return df

        def _cell(v: object) -> str:
            try:
                x = float(v)
            except Exception:
                return ""
            if x > 0:
                return "color: #ff4d4f;"
            if x < 0:
                return "color: #52c41a;"
            return ""

        return df.style.format("{:,.0f}", subset=diff_cols).map(_cell, subset=diff_cols)
    except Exception:
        return df


st.set_page_config(page_title="庫存銷售差異", layout="wide")
st.title("庫存／銷售 雙檔比對")
st.caption(
    "兩份 Excel 需能對應到 **條碼／門市／庫存／銷售**（可接受常見欄名別名）。"
    " 以 (條碼, 門市) 合併；差異 = **系統 − 客戶**。同檔內重複列會先加總。"
)

s1, s2 = st.columns(2)
with s1:
    st.caption("系統（基準）")
    sys_file = st.file_uploader("系統檔", type=["xlsx", "xls"], key="sys")
with s2:
    st.caption("客戶")
    cust_file = st.file_uploader("客戶檔", type=["xlsx", "xls"], key="cust")

only_diff = st.checkbox("僅顯示庫存或銷售有差異的列", value=True)

if sys_file is None or cust_file is None:
    st.info("請上傳 **系統檔** 與 **客戶檔** 各一份。")
    st.stop()

try:
    raw_s = pd.read_excel(sys_file)
    raw_c = pd.read_excel(cust_file)
    sys_df = vf.load_simple_inventory_sales(raw_s)
    cust_df = vf.load_simple_inventory_sales(raw_c)

    if len(sys_df) == 0 or len(cust_df) == 0:
        st.warning("其中一份檔案沒有有效資料列（需有 條碼+門市）。")
        st.stop()

    rep_full = vf.compute_simple_diff_report(system_df=sys_df, customer_df=cust_df)
    if only_diff:
        rep = rep_full[
            (rep_full["庫存差異"].abs() > 1e-9) | (rep_full["銷售差異"].abs() > 1e-9)
        ].reset_index(drop=True)
    else:
        rep = rep_full

    st.subheader("差異報表" + ("（僅有差異）" if only_diff else "（全部鍵）"))
    st.caption(f"列數：{len(rep):,}（全部合併列數 {len(rep_full):,}）")
    st.dataframe(
        _style_numbers_pos_red_neg_green(rep),
        use_container_width=True,
        hide_index=True,
    )

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        rep.to_excel(w, sheet_name="差異", index=False)
        if only_diff and len(rep) != len(rep_full):
            rep_full.to_excel(w, sheet_name="完整合併", index=False)
    buf.seek(0)
    st.download_button(
        "下載 Excel",
        data=buf.getvalue(),
        file_name="庫存銷售差異.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
except Exception as e:
    st.error(str(e))
