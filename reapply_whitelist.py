"""
从 Cleaned_Biotech_List_Checkpoint.csv 按当前白名单重新应用过滤，写回 Final_Non_Oncology_Pharma.csv。
不拉 yfinance，用于白名单更新后快速得到 423 家。
"""
import json
import re
import pathlib
import pandas as pd

DATA_DIR = pathlib.Path(__file__).parent
CHECKPOINT = DATA_DIR / "Cleaned_Biotech_List_Checkpoint.csv"
OUTPUT = DATA_DIR / "Final_Non_Oncology_Pharma.csv"

NON_PHARMA_INDUSTRIES = ["medical devices", "diagnostics", "instruments", "health information"]
NON_PHARMA_KEYWORDS = [r"medical device", r"diagnostic", r"surgical", r"instrument", r"equipment", r"contract research", r"CRO", r"analytical", r"packaging"]
ONCOLOGY_KEYWORDS = [r"oncology", r"cancer", r"tumor", r"carcinoma", r"leukemia", r"lymphoma", r"myeloma", r"melanoma", r"sarcoma", r"glioblastoma", r"malignancy", r"CAR-T", r"ADC", r"antibody-drug conjugate"]
non_pharma_re = re.compile(r"\b(" + "|".join(NON_PHARMA_KEYWORDS) + r")\b", re.IGNORECASE)
oncology_re = re.compile(r"\b(" + "|".join(ONCOLOGY_KEYWORDS) + r")\b", re.IGNORECASE)

# 白名单
WHITELIST_JSON = DATA_DIR / "whitelist_symbols.json"
if WHITELIST_JSON.exists():
    with open(WHITELIST_JSON, encoding="utf-8") as f:
        _wl = json.load(f)
    WHITELIST_SYMBOLS = {str(e.get("symbol", "")).strip().upper() for e in _wl if e.get("symbol")}
else:
    WHITELIST_SYMBOLS = set()

def should_drop(row):
    sym = str(row.get("Symbol", "")).strip().upper()
    if sym in WHITELIST_SYMBOLS:
        return False
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

def main():
    raw = pd.read_csv(CHECKPOINT)
    drop_mask = raw.apply(should_drop, axis=1)
    final = raw[~drop_mask].copy()
    final.reset_index(drop=True, inplace=True)
    final.to_csv(OUTPUT, index=False, encoding="utf-8-sig")
    print("  Whitelist:", sorted(WHITELIST_SYMBOLS))
    print(f"  Checkpoint {len(raw)} -> after filter {len(final)} -> saved to {OUTPUT.name}")

if __name__ == "__main__":
    main()
