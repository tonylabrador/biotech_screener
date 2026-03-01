"""
update_financials.py — 批量刷新生物制药公司最新财务数据
输入/输出: 当前目录下的 Biotech_Pipeline_Master.csv
依赖: pandas, yfinance, tqdm, concurrent.futures (标准库)
"""

import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import pandas as pd
import yfinance as yf
from tqdm import tqdm

# ── 配置 ──────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).resolve().parent
MASTER_CSV = SCRIPT_DIR / "Biotech_Pipeline_Master.csv"
MAX_WORKERS = 16
ENCODING = "utf-8-sig"


def fetch_financials(symbol: str) -> Optional[dict]:
    """
    从 yfinance 抓取单只股票的 Price、Market Cap、Total Cash。
    返回 None 表示抓取失败（退市/API 错误等），不抛异常。
    """
    if not symbol or str(symbol).strip() == "":
        return None
    symbol = str(symbol).strip().upper()
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        if not info:
            return None
        # Price: currentPrice 优先，否则 regularMarketPrice
        price = None
        if info.get("currentPrice") is not None:
            try:
                price = float(info["currentPrice"])
            except (TypeError, ValueError):
                pass
        if price is None and info.get("regularMarketPrice") is not None:
            try:
                price = float(info["regularMarketPrice"])
            except (TypeError, ValueError):
                pass
        # Market Cap
        mcap = None
        if info.get("marketCap") is not None:
            try:
                mcap = float(info["marketCap"])
            except (TypeError, ValueError):
                pass
        # Total Cash (可选)
        cash = None
        if info.get("totalCash") is not None:
            try:
                cash = float(info["totalCash"])
            except (TypeError, ValueError):
                pass
        return {
            "Symbol": symbol,
            "Price": price,
            "Market Cap": mcap,
            "Total Cash": cash,
        }
    except Exception:
        return None


def main() -> int:
    if not MASTER_CSV.exists():
        print(f"Error: {MASTER_CSV} not found.", file=sys.stderr)
        return 1

    df = pd.read_csv(MASTER_CSV, encoding=ENCODING)
    if "Symbol" not in df.columns:
        print("Error: CSV must contain a 'Symbol' column.", file=sys.stderr)
        return 1

    symbols = df["Symbol"].dropna().astype(str).str.strip().str.upper().unique().tolist()
    if not symbols:
        print("No symbols found in Symbol column.")
        return 0

    print(f"Found {len(symbols)} unique symbols. Fetching financials (workers={MAX_WORKERS})...")
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_symbol = {executor.submit(fetch_financials, s): s for s in symbols}
        for future in tqdm(as_completed(future_to_symbol), total=len(symbols), desc="Fetching", unit="sym"):
            sym = future_to_symbol[future]
            try:
                row = future.result()
                if row is not None:
                    results.append(row)
            except Exception:
                pass

    if not results:
        print("No data fetched. Exiting without overwriting CSV.")
        return 0

    # 映射回原表：按 Symbol 更新 Price, Market Cap, Total Cash（纯 float）
    # 仅用抓取到的值覆盖，未抓取到的 Symbol 保留原值
    upd = pd.DataFrame(results)
    df["_sym_norm"] = df["Symbol"].astype(str).str.strip().str.upper()
    for col in ["Price", "Market Cap", "Total Cash"]:
        if col not in df.columns:
            df[col] = pd.NA
        sym_to_val = upd.set_index("Symbol")[col].to_dict()
        new_vals = df["_sym_norm"].map(sym_to_val)
        df[col] = new_vals.where(new_vals.notna(), df[col])
    df.drop(columns=["_sym_norm"], inplace=True)
    # 保证数值列为 float，便于后续排序/比对
    for col in ["Price", "Market Cap", "Total Cash"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df.to_csv(MASTER_CSV, encoding=ENCODING, index=False)
    print(f"Updated {MASTER_CSV} with {len(results)} symbols.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
