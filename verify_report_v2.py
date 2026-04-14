from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


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
    cols: ReportRuleCols = ReportRuleCols(),
    tolerance: float = 0.0,
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
    x_sales = get_or_zero(cols.sales)
    y = _to_num(out[target_col])

    expected = x_stock - x_pur + x_ret - x_sales
    delta = expected - y.fillna(0)

    ok = delta.abs() <= float(tolerance)
    issue = pd.Series("", index=out.index, dtype="object")
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
    args = ap.parse_args()

    xlsx_path = Path(args.xlsx).expanduser().resolve()
    if not xlsx_path.exists():
        raise SystemExit(f"檔案不存在: {xlsx_path}")

    xl = pd.ExcelFile(xlsx_path)
    sheet = args.sheet if args.sheet else xl.sheet_names[0]
    df = pd.read_excel(xlsx_path, sheet_name=sheet)
    checked = check_report_v2(df, tolerance=float(args.tolerance))

    out_xlsx = xlsx_path.with_name(f"{xlsx_path.stem}_checked.xlsx")
    out_csv = xlsx_path.with_name(f"{xlsx_path.stem}_issues.csv")
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        checked.to_excel(w, index=False, sheet_name=sheet)
        issues = checked.loc[checked["__issue"].astype(str) != "", :].copy()
        issues.to_excel(w, index=False, sheet_name="issues")

    checked.loc[checked["__issue"].astype(str) != "", :].to_csv(out_csv, index=False, encoding="utf-8-sig")

    issues_n = int((checked["__issue"].astype(str) != "").sum())
    ok_n = int(checked["__ok"].sum())
    total_n = int(len(checked))
    print(f"完成：total={total_n} ok={ok_n} issues={issues_n}")
    print(f"輸出：{out_xlsx}")
    print(f"輸出：{out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

