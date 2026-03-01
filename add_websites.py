"""
add_websites.py — 为 Company_Pipeline_Summary.csv 补充官网 + IR 智能研报搜索链接
依赖: pandas, yfinance, tqdm
"""

import sys
import time
from pathlib import Path
from urllib.parse import quote_plus, urlparse

import pandas as pd
import yfinance as yf
from tqdm import tqdm

SCRIPT_DIR = Path(__file__).resolve().parent
CSV_PATH = SCRIPT_DIR / "Company_Pipeline_Summary.csv"
ENCODING = "utf-8-sig"


def extract_domain(url: str) -> str:
    """https://www.ucb.com/path -> ucb.com"""
    if not url:
        return ""
    try:
        host = urlparse(url).netloc or urlparse("https://" + url).netloc
        host = host.lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def build_ir_link(domain: str) -> str:
    q = f'site:{domain} filetype:pdf "corporate presentation" OR "investor presentation"'
    return f"https://www.google.com/search?q={quote_plus(q)}"


def fetch_website(symbol: str) -> str:
    try:
        info = yf.Ticker(symbol).info
        if info:
            return info.get("website", "") or ""
    except Exception:
        pass
    return ""


def main() -> int:
    if not CSV_PATH.exists():
        print(f"Error: {CSV_PATH} not found.", file=sys.stderr)
        return 1

    df = pd.read_csv(CSV_PATH, encoding=ENCODING)
    if "Symbol" not in df.columns:
        print("Error: CSV must contain a 'Symbol' column.", file=sys.stderr)
        return 1

    symbols = df["Symbol"].dropna().astype(str).str.strip().unique().tolist()
    print(f"Found {len(symbols)} unique symbols. Fetching websites...")

    sym_to_site: dict[str, str] = {}
    for sym in tqdm(symbols, desc="Fetching", unit="sym"):
        sym_to_site[sym.upper()] = fetch_website(sym)
        time.sleep(0.1)

    df["_sym_upper"] = df["Symbol"].astype(str).str.strip().str.upper()
    df["Website"] = df["_sym_upper"].map(sym_to_site).fillna("")
    df["_domain"] = df["Website"].apply(extract_domain)
    df["IR_Search_Link"] = df["_domain"].apply(
        lambda d: build_ir_link(d) if d else "N/A"
    )
    df.drop(columns=["_sym_upper", "_domain"], inplace=True)

    df.to_csv(CSV_PATH, encoding=ENCODING, index=False)
    filled = (df["Website"].str.len() > 0).sum()
    print(f"Done. {filled}/{len(df)} rows have Website. Saved → {CSV_PATH.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
