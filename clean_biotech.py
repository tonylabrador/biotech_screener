"""
Biotech stock list: merge, clean & filter pipeline
───────────────────────────────────────────────────
Source : 3 x Seeking Alpha xlsx exports
Enrich : yfinance (Industry + Business Summary)
Output : Final_Non_Oncology_Pharma.csv
"""

import io
import json
import re
import sys
import time
import pathlib
import pandas as pd
import yfinance as yf
from tqdm import tqdm

# Force UTF-8 stdout on Windows so Chinese / special chars don't crash
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace"
    )
    sys.stderr = io.TextIOWrapper(
        sys.stderr.buffer, encoding="utf-8", errors="replace"
    )

# ═══════════════════════════════════════════════════════════════
# 1. 读取 & 合并
# ═══════════════════════════════════════════════════════════════
DATA_DIR = pathlib.Path(__file__).parent

FILES = [
    DATA_DIR / "all pharma_biotech_1 2026-02-26.xlsx",
    DATA_DIR / "all pharma_biotech_2 2026-02-26.xlsx",
    DATA_DIR / "all pharma_biotech_3 2026-02-26.xlsx",
]

dfs = []
for f in FILES:
    df_part = pd.read_excel(f, engine="calamine")
    dfs.append(df_part)
    print(f"  读取 {f.name}  →  {len(df_part)} 行")

raw = pd.concat(dfs, ignore_index=True)
total_read = len(raw)
print(f"\n合并后共 {total_read} 行")

raw.drop_duplicates(subset="Symbol", keep="first", inplace=True)
after_dedup_sym = len(raw)
print(f"Symbol 去重后共 {after_dedup_sym} 行（去除 {total_read - after_dedup_sym} 个重复 Symbol）")

# ── Deduplicate by company Name: keep the most "US-primary" ticker ──

def _ticker_priority(sym: str) -> int:
    """Lower = better. US primary > ADR (Y) > OTC foreign (F) > others."""
    s = str(sym).strip()
    has_dot_colon = ("." in s) or (":" in s)
    if has_dot_colon:
        return 3  # e.g. DHT.U:CA, KRKA.F
    if len(s) >= 4 and s[-1] == "F":
        return 2  # OTC foreign sheet (NONOF, ARGNF …)
    if len(s) >= 4 and s[-1] == "Y":
        return 1  # ADR (UCBJY, CSLLY …)
    return 0      # US primary (NVO, ARGX, HLN …)

raw["_tp"] = raw["Symbol"].apply(_ticker_priority)
raw.sort_values("_tp", inplace=True)
before_name_dedup = len(raw)
raw.drop_duplicates(subset="Name", keep="first", inplace=True)
raw.drop(columns="_tp", inplace=True)
after_dedup = len(raw)
print(f"公司名去重后共 {after_dedup} 行（去除 {before_name_dedup - after_dedup} 个重复 ticker）")

raw.reset_index(drop=True, inplace=True)

# ═══════════════════════════════════════════════════════════════
# 2. yfinance 补充抓取
# ═══════════════════════════════════════════════════════════════
CHECKPOINT = DATA_DIR / "Cleaned_Biotech_List_Checkpoint.csv"

industries = []
summaries = []

symbols = raw["Symbol"].tolist()

print(f"\n开始从 yfinance 抓取 {len(symbols)} 家公司数据…\n")

for i, sym in enumerate(tqdm(symbols, desc="yfinance 抓取", unit="家")):
    try:
        info = yf.Ticker(sym).info
        industries.append(info.get("industry", "N/A"))
        summaries.append(info.get("longBusinessSummary", "N/A") or "N/A")
    except Exception:
        industries.append("N/A")
        summaries.append("N/A")

    time.sleep(0.3)

    if (i + 1) % 50 == 0:
        raw_cp = raw.iloc[: i + 1].copy()
        raw_cp["Industry"] = industries
        raw_cp["Business Summary"] = summaries
        raw_cp.to_csv(CHECKPOINT, index=False, encoding="utf-8-sig")
        tqdm.write(f"  ✓ 已保存 checkpoint（前 {i + 1} 家）→ {CHECKPOINT.name}")

raw["Industry"] = industries
raw["Business Summary"] = summaries

raw.to_csv(CHECKPOINT, index=False, encoding="utf-8-sig")
print(f"\n全量 checkpoint 已保存 → {CHECKPOINT.name}")

# ═══════════════════════════════════════════════════════════════
# 3. 双重过滤
# ═══════════════════════════════════════════════════════════════
NON_PHARMA_INDUSTRIES = [
    "medical devices",
    "diagnostics",
    "instruments",
    "health information",
]

NON_PHARMA_KEYWORDS = [
    r"medical device", r"diagnostic", r"surgical",
    r"instrument", r"equipment", r"contract research",
    r"CRO", r"analytical", r"packaging",
]

ONCOLOGY_KEYWORDS = [
    r"oncology", r"cancer", r"tumor", r"carcinoma",
    r"leukemia", r"lymphoma", r"myeloma", r"melanoma",
    r"sarcoma", r"glioblastoma", r"malignancy",
    r"CAR-T", r"ADC", r"antibody-drug conjugate",
]

non_pharma_re = re.compile(
    r"\b(" + "|".join(NON_PHARMA_KEYWORDS) + r")\b",
    re.IGNORECASE,
)
oncology_re = re.compile(
    r"\b(" + "|".join(ONCOLOGY_KEYWORDS) + r")\b",
    re.IGNORECASE,
)

# ── 白名单：即使触发过滤规则也强制保留 ────────────────────────────
WHITELIST_JSON = DATA_DIR / "whitelist_symbols.json"
if WHITELIST_JSON.exists():
    with open(WHITELIST_JSON, encoding="utf-8") as f:
        _wl = json.load(f)
    WHITELIST_SYMBOLS = {str(e.get("symbol", "")).strip().upper() for e in _wl if e.get("symbol")}
    print(f"  白名单已加载：{sorted(WHITELIST_SYMBOLS)}")
else:
    WHITELIST_SYMBOLS = set()
    print(f"  白名单文件 {WHITELIST_JSON.name} 不存在，跳过")


def should_drop(row: pd.Series) -> bool:
    sym = str(row.get("Symbol", "")).strip().upper()
    if sym in WHITELIST_SYMBOLS:
        return False  # 白名单公司永不剔除

    industry = str(row.get("Industry", "")).lower()
    summary = str(row.get("Business Summary", ""))

    for kw in NON_PHARMA_INDUSTRIES:
        if kw in industry:
            return True

    if non_pharma_re.search(summary):
        return True

    if oncology_re.search(summary):
        return True

    return False


drop_mask = raw.apply(should_drop, axis=1)
final = raw[~drop_mask].copy()
final.reset_index(drop=True, inplace=True)

# ═══════════════════════════════════════════════════════════════
# 4. 输出
# ═══════════════════════════════════════════════════════════════
OUTPUT = DATA_DIR / "Final_Non_Oncology_Pharma.csv"
final.to_csv(OUTPUT, index=False, encoding="utf-8-sig")

print("\n" + "═" * 60)
print(f"  总计读取        {total_read} 家")
print(f"  合并去重后      {after_dedup} 家")
print(f"  执行过滤后最终保留 {len(final)} 家纯生物制药公司")
print("═" * 60)
print(f"\n最终文件已保存 → {OUTPUT.name}")
