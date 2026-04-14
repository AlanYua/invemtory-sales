from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

import sales_reports as sr


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


@dataclass(frozen=True)
class ReportRuleCols:
    diff_stock: str = "差異(庫存)"
    diff_purchase: str = "差異(進貨)"
    diff_return: str = "差異(退貨)"
    sales: str = "當月累計銷售"
    target_prefix: str = "差異數("


def check_report_v2(
    df: pd.DataFrame,
    *,
    sales_by_ean: pd.DataFrame | None = None,
    sales_override: bool = True,
    cols: ReportRuleCols = ReportRuleCols(),
    tolerance: float = 0.0,
    do_check: bool = True,
) -> pd.DataFrame:
    """
    規則：庫存差異-進貨+退貨-銷售
      expected = 差異(庫存) - 差異(進貨) + 差異(退貨) - 當月累計銷售
    比對：expected vs 差異數(...)

    規則約定：
      - 來源都是上傳的檔案，沒有/空白一律視為 0
      - 只要 expected 跟 差異數(...) 不一致就算錯
    """
    d = _normalize_columns(df)

    target_cols = [c for c in d.columns if str(c).startswith(cols.target_prefix)]
    if len(target_cols) != 1:
        raise ValueError(f"找不到唯一的目標欄（以 {cols.target_prefix!r} 開頭），目前找到: {target_cols}")
    target_col = target_cols[0]

    out = d.copy()

    def get_or_zero(col: str) -> pd.Series:
        # 來源都是上傳檔案：欄位不存在/空白都視為 0
        if col not in out.columns:
            return pd.Series([0] * len(out), index=out.index, dtype="float64")
        return _to_num(out[col]).fillna(0)

    x_stock = get_or_zero(cols.diff_stock)
    x_pur = get_or_zero(cols.diff_purchase)
    x_ret = get_or_zero(cols.diff_return)

    # 當月累計銷售：可由銷售統計帶入（依 EAN 加總），沒有則視為 0
    if sales_by_ean is not None and len(sales_by_ean) and "EAN" in sales_by_ean.columns:
        s = sales_by_ean.copy()
        if "sales_qty" not in s.columns:
            raise ValueError("sales_by_ean 需要欄位: EAN, sales_qty")
        s["EAN"] = s["EAN"].astype(str).str.strip()
        s["sales_qty"] = pd.to_numeric(s["sales_qty"], errors="coerce").fillna(0)
        out["EAN"] = out.get("EAN", "").astype(str).str.strip()
        m = out.merge(s[["EAN", "sales_qty"]], on="EAN", how="left")
        # merge 會多出一個 sales_qty 欄，將它回填到 out
        sales_in = pd.to_numeric(m["sales_qty"], errors="coerce").fillna(0)
        if sales_override or cols.sales not in out.columns:
            out[cols.sales] = sales_in
        else:
            # 不覆蓋：只在原本欄位缺值時補上
            cur = _to_num(out[cols.sales]).fillna(0)
            out[cols.sales] = cur.where(_to_num(out[cols.sales]).notna(), sales_in)

    x_sales = get_or_zero(cols.sales)
    y = _to_num(out[target_col])

    expected = x_stock - x_pur + x_ret - x_sales
    delta = expected - y.fillna(0)

    ok = delta.abs() <= float(tolerance) if do_check else True
    issue = pd.Series("", index=out.index, dtype="object")
    if do_check:
        issue = issue.mask(~ok, "差異數不符")

    out["__expected(庫存差異-進貨+退貨-銷售)"] = expected
    out["__delta(expected-差異數)"] = delta
    out["__ok"] = ok
    out["__issue"] = issue
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("xlsx", help="要查驗的 verify_v2.xlsx 檔案路徑")
    ap.add_argument("--sheet", default=None, help="工作表名稱（預設讀第一張）")
    ap.add_argument("--tolerance", type=float, default=0.0, help="允許誤差（預設 0）")
    ap.add_argument("--sales-file", default=None, help="銷售統計上傳檔（xlsx/csv），用來帶出當月累計銷售")
    ap.add_argument("--sales-sheet", default=None, help="銷售統計工作表名稱（預設第一張）")
    ap.add_argument("--customer", default=None, help="客戶名稱（用來篩選銷售統計）")
    ap.add_argument("--report-date-from", default=None, help="銷售區間起（YYYY-MM-DD，含）")
    ap.add_argument("--report-date-to", default=None, help="銷售區間迄（YYYY-MM-DD，含）")
    ap.add_argument("--sales-override", action="store_true", help="強制以銷售統計覆蓋『當月累計銷售』欄")
    ap.add_argument(
        "--no-check",
        action="store_true",
        help="只帶入『當月累計銷售』並計算 expected，不做差異數比對/不輸出 issues",
    )
    args = ap.parse_args()

    xlsx_path = Path(args.xlsx).expanduser().resolve()
    if not xlsx_path.exists():
        raise SystemExit(f"檔案不存在: {xlsx_path}")

    xl = pd.ExcelFile(xlsx_path)
    sheet = args.sheet if args.sheet else xl.sheet_names[0]
    df = pd.read_excel(xlsx_path, sheet_name=sheet)

    sales_by_ean = None
    if args.sales_file:
        sales_path = Path(args.sales_file).expanduser().resolve()
        if not sales_path.exists():
            raise SystemExit(f"銷售檔不存在: {sales_path}")
        if sales_path.suffix.lower() in {".csv"}:
            raw = pd.read_csv(sales_path)
        else:
            sx = pd.ExcelFile(sales_path)
            ss = args.sales_sheet if args.sales_sheet else sx.sheet_names[0]
            raw = pd.read_excel(sales_path, sheet_name=ss)
        sales_df = sr.load_sales(raw)

        if args.customer:
            sales_df = sales_df[sales_df["customer"].astype(str).str.strip() == str(args.customer).strip()]
        if args.report_date_from:
            start = pd.to_datetime(args.report_date_from, errors="coerce")
            if pd.isna(start):
                raise SystemExit("--report-date-from 無法解析")
            sales_df = sales_df[pd.to_datetime(sales_df["report_date"]).dt.normalize() >= pd.Timestamp(start).normalize()]
        if args.report_date_to:
            end = pd.to_datetime(args.report_date_to, errors="coerce")
            if pd.isna(end):
                raise SystemExit("--report-date-to 無法解析")
            sales_df = sales_df[pd.to_datetime(sales_df["report_date"]).dt.normalize() <= pd.Timestamp(end).normalize()]

        sales_by_ean = (
            sales_df.groupby("EAN", as_index=False)["qty"].sum().rename(columns={"qty": "sales_qty"})
            if len(sales_df)
            else pd.DataFrame(columns=["EAN", "sales_qty"])
        )

    checked = check_report_v2(
        df,
        tolerance=float(args.tolerance),
        sales_by_ean=sales_by_ean,
        sales_override=bool(args.sales_override),
        do_check=not bool(args.no_check),
    )

    out_xlsx = xlsx_path.with_name(f"{xlsx_path.stem}_checked.xlsx")
    out_csv = xlsx_path.with_name(f"{xlsx_path.stem}_issues.csv")
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        checked.to_excel(w, index=False, sheet_name=sheet)
        if not bool(args.no_check):
            issues = checked.loc[checked["__issue"].astype(str) != "", :].copy()
            issues.to_excel(w, index=False, sheet_name="issues")

    if not bool(args.no_check):
        checked.loc[checked["__issue"].astype(str) != "", :].to_csv(
            out_csv, index=False, encoding="utf-8-sig"
        )

    total_n = int(len(checked))
    if bool(args.no_check):
        print(f"完成：total={total_n}（no-check）")
    else:
        issues_n = int((checked["__issue"].astype(str) != "").sum())
        ok_n = int(checked["__ok"].sum())
        print(f"完成：total={total_n} ok={ok_n} issues={issues_n}")
    print(f"輸出：{out_xlsx}")
    if not bool(args.no_check):
        print(f"輸出：{out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

