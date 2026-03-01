"""
Build a per-asset pipeline master table from Enriched_Clinical_Trials.csv
─────────────────────────────────────────────────────────────────────────
1. Ultra-clean drug name extraction (strip prefixes, doses, routes, controls)
2. Groups by (Symbol, Company, Asset) — phase / condition / catalyst aggregation
3. Calls Gemini to classify TA + infer MoA in a single batch call
4. Calls Gemini to classify Market Status (Marketed / Investigational)
5. Generates Company_Pipeline_Summary.csv merged with pharma list

Input  : Enriched_Clinical_Trials.csv
Output : Biotech_Pipeline_Master.csv, Company_Pipeline_Summary.csv
"""

import io
import json
import os
import re
import sys
import time
import pathlib
from datetime import datetime

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from google import genai
from tqdm import tqdm

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace"
    )
    sys.stderr = io.TextIOWrapper(
        sys.stderr.buffer, encoding="utf-8", errors="replace"
    )

# ── Paths & Gemini ────────────────────────────────────────────

DATA_DIR = pathlib.Path(__file__).parent
INPUT_CSV = DATA_DIR / "Enriched_Clinical_Trials.csv"
OUTPUT_CSV = DATA_DIR / "Biotech_Pipeline_Master.csv"
PHARMA_CSV = DATA_DIR / "Final_Non_Oncology_Pharma.csv"
COMPANY_SUMMARY_CSV = DATA_DIR / "Company_Pipeline_Summary.csv"

# ── Therapeutic Area 规范化（合并/改名，不删公司）；以固定列表为准，不再新增 TA ───
CANONICAL_TA = frozenset([
    "Cardiovascular", "Dermatology", "Gastroenterology", "Hematology",
    "Immunology", "Infectious Diseases", "Metabolic/Endocrinology", "Musculoskeletal",
    "Neurology/CNS", "No Trials", "Ophthalmology", "Others", "Pain", "Rare Disease",
    "Respiratory", "Urology/Nephrology",
])


def _normalize_ta(ta: str) -> str:
    """将 TA 规范为统一名称；不在固定列表内的（含 Reproductive Health, Rheumatology, Unclassified）→ Others。"""
    if not ta or not str(ta).strip():
        return ""
    t = str(ta).strip()
    # 1) Parkinson's disease、Psychiatry → Neurology/CNS
    if "parkinson" in t.lower() or t == "Psychiatry":
        return "Neurology/CNS"
    # 2) Rare/Orphan → Rare Disease
    if "rare" in t.lower() and ("orphan" in t.lower() or "/" in t):
        return "Rare Disease"
    if t == "Rare/Orphan Diseases":
        return "Rare Disease"
    # 3) Pain/Analgesia → Pain
    if "pain" in t.lower() or "analgesia" in t.lower():
        return "Pain"
    # 4) Endocrinology  standalone 或 Endocrinology/Metabolic → Metabolic/Endocrinology
    if t == "Endocrinology" or t == "Endocrinology/Metabolic":
        return "Metabolic/Endocrinology"
    # 5) Immunology/Autoimmune → Immunology
    if t == "Immunology/Autoimmune":
        return "Immunology"
    # 6) 仅允许固定列表；Reproductive Health, Rheumatology, Unclassified 及任何未列出的 → Others
    if t not in CANONICAL_TA:
        return "Others"
    return t


load_dotenv(DATA_DIR / ".env")
_gemini_client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY", ""))

_MD_FENCE = re.compile(r"^```(?:\w+)?\s*\n?|\n?```\s*$", re.MULTILINE)

BATCH_SIZE = 20
GEMINI_MODEL = "gemini-2.5-flash-lite"
TODAY = datetime.now()

# ══════════════════════════════════════════════════════════════
# Phase priority
# ══════════════════════════════════════════════════════════════

PHASE_RANK = {
    "PHASE4": 5,
    "PHASE3": 4,
    "PHASE2/PHASE3": 3.5,
    "PHASE2": 3,
    "PHASE1/PHASE2": 2.5,
    "PHASE1": 2,
    "EARLY_PHASE1": 1,
    "NA": 0,
    "N/A": 0,
}


def _highest_phase(phases_series: pd.Series) -> str:
    best_rank = -1
    best_label = "N/A"
    for raw in phases_series.dropna():
        for token in str(raw).split(", "):
            token = token.strip().upper()
            rank = PHASE_RANK.get(token, -1)
            if rank > best_rank:
                best_rank = rank
                best_label = token
    return best_label


# ══════════════════════════════════════════════════════════════
# Drug name cleaning — ultra-clean extraction
# ══════════════════════════════════════════════════════════════

_DRUG_PREFIX = re.compile(
    r"^(DRUG:\s*|BIOLOGICAL:\s*|DEVICE:\s*|PROCEDURE:\s*|"
    r"DIETARY SUPPLEMENT:\s*|BEHAVIORAL:\s*|RADIATION:\s*|"
    r"DIAGNOSTIC TEST:\s*|GENETIC:\s*|COMBINATION PRODUCT:\s*|OTHER:\s*)",
    re.IGNORECASE,
)

_TRAILING_NOISE = re.compile(
    r"\s*[\(\[]?\s*"
    r"\d+[\.\d]*\s*"
    r"(mg|mcg|ug|μg|mg/kg|mg/m2|ml|units?|IU|mIU|mmol|g|kg)"
    r"(/\w+)?"
    r"[\)\]]?\s*$"
    r"|"
    r"\s*,?\s*\b(oral|orally|iv|i\.v\.|intravenous|intravenously|"
    r"sc|s\.c\.|subcutaneous|subcutaneously|"
    r"im|i\.m\.|intramuscular|intramuscularly|"
    r"topical|topically|"
    r"tablet|tablets|capsule|capsules|"
    r"injection|infusion|solution|suspension|"
    r"ophthalmic|nasal|inhaled|inhalation|"
    r"patch|cream|ointment|gel|spray|drops?|"
    r"extended[- ]release|modified[- ]release|"
    r"once daily|twice daily|BID|QD|TID)\b\s*$",
    re.IGNORECASE,
)

_SKIP_TOKENS = re.compile(
    r"^("
    r"placebo(\s+.*)?|.*\bplacebo\b.*|"
    r"sham|standard\s+of\s+care|SOC|"
    r"best\s+supportive\s+care|BSC|"
    r"no\s+intervention|observation|watchful\s+waiting|"
    r"saline|normal\s+saline|inactive|"
    r"comparator|active\s+comparator|control|"
    r"dummy|usual\s+care|routine\s+care"
    r")$",
    re.IGNORECASE,
)

# 明确识别 “MATCHING PLACEBO / PLACEBO MATCHING” 及各类 placebo 变体，用于 asset 过滤（避免进入 Master CSV）
_PLACEBO_ASSET_PATTERN = re.compile(
    r"^(.*\bmatching\s+placebo\b|.*\bplacebo\s+matching\b|"
    r"placebo\s+for\s+|placebo\s+to\s+match|placebo\s+of\s+|placebo\s*\(|"
    r".*\bplacebo\s+control\b|.*\bplacebo\s+comparator\b|.*\bthe\s+placebo\b|"
    r".*\bplacebo\s+matched\b|.*\bmatched\s+placebo\b|.*\bplacebo\s+in\s+|"
    r".*\bplacebo\s+(booster|vaccine|pen\b|injector|syringe|DPI|auto-injector)\b|"
    r".*vehicle\s*\(?\s*placebo|.*\s+or\s+placebo\s*$|.*/placebo\s*$|"
    r".*-placebo\s*$|.*\s+placebo\s*$|.*\bplacebo\b.*)$",
    re.IGNORECASE,
)


def is_placebo_asset(name: str) -> bool:
    """Return True if asset name is a placebo/control variant (matching placebo, placebo for X, X placebo, etc.)."""
    if not name or not str(name).strip():
        return False
    return bool(_PLACEBO_ASSET_PATTERN.match(str(name).strip()))


_PAREN_CONTENT = re.compile(r"\s*\([^)]*\)\s*")

# 清洗 TREATMENT 类型前缀/后缀：Open Label, Blinded, Open-Label, High/Low Dose 等（与 dashboard 一致）
_TREATMENT_STRIP_LEADING = re.compile(
    r"^\s*(?:open\s*[-]?\s*label\s+(?:extension\s*[-]?\s*period\s*)?|blinded\s+|double\s*[-]?\s*blind(?:ed)?\s+|single\s*[-]?\s*blind(?:ed)?\s+|"
    r"high\s+dose\s+of\s+|low\s+dose\s+of\s+|high\s+dose\s+|low\s+dose\s+)+",
    re.IGNORECASE,
)
_TREATMENT_STRIP_TRAILING = re.compile(
    r"\s+(?:open\s*[-]?\s*label|blinded|double\s*[-]?\s*blind|single\s*[-]?\s*blind|high\s+dose|low\s+dose)\s*$",
    re.IGNORECASE,
)


def _strip_treatment_prefix_suffix(s: str) -> str:
    if not s or not str(s).strip():
        return s
    t = str(s).strip()
    while _TREATMENT_STRIP_LEADING.search(t):
        t = _TREATMENT_STRIP_LEADING.sub("", t, count=1).strip()
    while _TREATMENT_STRIP_TRAILING.search(t):
        t = _TREATMENT_STRIP_TRAILING.sub("", t, count=1).strip()
    return t.strip() or s


def normalize_asset_for_grouping(asset_name: str) -> str:
    """
    将同一药物的多种写法归一为同一 key，用于 pipeline 按 asset 合并。
    例如 Blinded ESK-001 / Open-Label ESK-001 / ESK 001 -> ESK-001
    """
    if not asset_name or not str(asset_name).strip():
        return str(asset_name).strip() or ""
    t = _strip_treatment_prefix_suffix(str(asset_name).strip())
    t = re.sub(r"\s+", " ", t).strip()
    # 统一 "ESK 001" 与 "ESK-001" 等形式：字母数字后接空格再接数字 -> 改为连字符
    t = re.sub(r"([A-Za-z0-9])\s+(\d)", r"\1-\2", t)
    return t.strip() or str(asset_name).strip()


def clean_drug_name(raw: str) -> str | None:
    """Extract the core active drug name from an intervention string."""
    name = _DRUG_PREFIX.sub("", raw).strip()
    name = _strip_treatment_prefix_suffix(name)
    name = _TRAILING_NOISE.sub("", name).strip()
    name = _TRAILING_NOISE.sub("", name).strip()
    name = name.strip(" ,;|/")
    if not name or len(name) < 2:
        return None
    if _SKIP_TOKENS.match(name):
        return None
    return name


def extract_assets(interventions_str: str) -> list[str]:
    """
    Split a comma/pipe-separated intervention string into clean asset names.
    Returns deduplicated list of active drugs; excludes placebo/control variants.
    """
    if pd.isna(interventions_str) or str(interventions_str).strip() in ("", "N/A"):
        return []
    parts = re.split(r"[|]", str(interventions_str))
    seen: set[str] = set()
    result: list[str] = []
    for p in parts:
        for sub in p.split(","):
            name = clean_drug_name(sub.strip())
            if name and not is_placebo_asset(name):
                key = name.upper()
                if key not in seen:
                    seen.add(key)
                    result.append(name)
    return result


# ══════════════════════════════════════════════════════════════
# Date parsing for Next Catalyst Date
# ══════════════════════════════════════════════════════════════

_DATE_FULL = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_DATE_YM = re.compile(r"^(\d{4})-(\d{2})$")


def _parse_date(raw: str) -> datetime | None:
    """Parse 'YYYY-MM-DD' or 'YYYY-MM' into a datetime (day defaults to last of month)."""
    if not raw or raw == "N/A":
        return None
    raw = str(raw).strip()
    m = _DATE_FULL.match(raw)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    m = _DATE_YM.match(raw)
    if m:
        try:
            y, mo = int(m.group(1)), int(m.group(2))
            if mo == 12:
                return datetime(y, 12, 31)
            return datetime(y, mo + 1, 1) - pd.Timedelta(days=1)
        except ValueError:
            return None
    return None


def _next_catalyst(dates_series: pd.Series) -> str:
    """
    From a series of PrimaryCompletionDate strings, find the nearest future date.
    Returns 'YYYY-MM-DD', 'Passed' if all in the past, or '' on parse failure.
    """
    future_dates: list[datetime] = []
    past_count = 0
    for raw in dates_series.dropna():
        dt = _parse_date(str(raw))
        if dt is None:
            continue
        if dt >= TODAY:
            future_dates.append(dt)
        else:
            past_count += 1
    if future_dates:
        nearest = min(future_dates)
        return nearest.strftime("%Y-%m-%d")
    if past_count > 0:
        return "Passed"
    return ""


# ══════════════════════════════════════════════════════════════
# Gemini — generic batch caller
# ══════════════════════════════════════════════════════════════

def _gemini_batch_call_raw(prompt: str) -> str | None:
    """Call Gemini and return stripped text, or None on failure."""
    try:
        resp = _gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
        )
        return _MD_FENCE.sub("", resp.text).strip()
    except Exception:
        return None


def _gemini_batch_call(prompt: str, n: int, fallback) -> list:
    """Generic batch Gemini call that returns a JSON array of n items."""
    raw = _gemini_batch_call_raw(prompt)
    if raw:
        try:
            results = json.loads(raw)
            if isinstance(results, list) and len(results) == n:
                return results
        except (json.JSONDecodeError, TypeError):
            pass
    return [fallback] * n


# ══════════════════════════════════════════════════════════════
# Gemini — TA + MoA (combined single batch call)
# ══════════════════════════════════════════════════════════════

_BATCH_TA_MOA_PROMPT = (
    "You are a senior medical and pharmacology expert.\n"
    "Below are items numbered 1 to {n}. Each item has format:\n"
    "  DRUG_NAME | CONDITIONS\n\n"
    "For EACH item, do TWO things:\n"
    "1. Classify the conditions into the 1-2 most relevant standard Therapeutic Areas.\n"
    "   The allowed TA pool is STRICTLY limited to:\n"
    "   Immunology/Autoimmune, Neurology/CNS, Metabolic/Endocrinology, "
    "Cardiovascular, Infectious Diseases, Rare/Orphan Diseases, "
    "Ophthalmology, Respiratory, Dermatology, Gastroenterology, "
    "Hematology, Musculoskeletal, Urology/Nephrology, "
    "Psychiatry, Pain/Analgesia, Others.\n"
    "2. Infer the drug's target or mechanism of action (MoA). "
    "Use concise pharmacology terms like 'GLP-1 receptor agonist', "
    "'IL-17A inhibitor', 'PD-1 antibody', 'JAK inhibitor', "
    "'factor Xa inhibitor', etc. If you genuinely cannot determine the MoA, "
    "return 'Unknown'.\n\n"
    "Return STRICTLY a JSON array of length {n}. Each element is an object:\n"
    '[{{"TA": "Area1, Area2", "MoA": "mechanism"}}, ...]\n'
    "No markdown, no explanation, no extra text. ONLY the JSON array."
)


def classify_ta_moa_batch(items: list[str]) -> tuple[list[str], list[str]]:
    """
    Classify TA and infer MoA for a batch of 'DRUG | CONDITIONS' strings.
    Returns (ta_list, moa_list).
    """
    n = len(items)
    numbered = "\n".join(f"{i+1}. {s}" for i, s in enumerate(items))
    prompt = _BATCH_TA_MOA_PROMPT.format(n=n) + "\n\n" + numbered

    raw = _gemini_batch_call_raw(prompt)
    if raw:
        try:
            results = json.loads(raw)
            if isinstance(results, list) and len(results) == n:
                tas = []
                moas = []
                for item in results:
                    if isinstance(item, dict):
                        tas.append(str(item.get("TA", "Unclassified")).strip())
                        moas.append(str(item.get("MoA", "Unknown")).strip())
                    else:
                        tas.append("Unclassified")
                        moas.append("Unknown")
                return tas, moas
        except (json.JSONDecodeError, TypeError):
            pass
    return ["Unclassified"] * n, ["Unknown"] * n


# ══════════════════════════════════════════════════════════════
# Gemini — Marketed drug classifier (batched)
# ══════════════════════════════════════════════════════════════

_BATCH_MARKETED_PROMPT = (
    "You are a senior drug regulatory expert.\n"
    "Below are items numbered 1 to {n}. Each has format:\n"
    "  DRUG_NAME | HIGHEST_PHASE | CONDITIONS\n\n"
    "For each, determine if the drug has been approved by FDA, EMA, or NMPA.\n"
    "Guidelines:\n"
    "- Phase 4 drugs are typically already marketed (post-marketing studies)\n"
    "- Names with ® or ™ are usually approved brand drugs\n"
    "- Use your pharmaceutical knowledge to distinguish approved drugs "
    "from investigational codes\n\n"
    "Return STRICTLY a JSON array of length {n}:\n"
    '["Marketed", "Investigational", ...]\n'
    "Each element must be exactly 'Marketed' or 'Investigational'.\n"
    "No markdown, no explanation. ONLY the JSON array."
)


def classify_marketed_batch(drug_info_list: list[str]) -> list[str]:
    n = len(drug_info_list)
    numbered = "\n".join(f"{i+1}. {d}" for i, d in enumerate(drug_info_list))
    prompt = _BATCH_MARKETED_PROMPT.format(n=n) + "\n\n" + numbered
    results = _gemini_batch_call(prompt, n, "Unknown")
    return [r if r in ("Marketed", "Investigational") else "Unknown" for r in results]


# ══════════════════════════════════════════════════════════════
# Company-level summary builder
# ══════════════════════════════════════════════════════════════

def _ticker_priority(sym: str) -> int:
    s = str(sym).strip()
    if ("." in s) or (":" in s):
        return 3
    if len(s) >= 4 and s[-1] == "F":
        return 2
    if len(s) >= 4 and s[-1] == "Y":
        return 1
    return 0


def _build_company_summary(pipeline: pd.DataFrame):
    """
    Aggregate pipeline master to company level, merge into pharma CSV.
    Outputs Company_Pipeline_Summary.csv with one row per (Company x TA).
    """
    if not PHARMA_CSV.exists():
        print(f"  WARNING: {PHARMA_CSV.name} not found, skipping company summary")
        return

    pharma = pd.read_csv(PHARMA_CSV)
    pharma = pharma.copy()
    pharma["_tp"] = pharma["Symbol"].apply(_ticker_priority)
    pharma.sort_values("_tp", inplace=True)
    pharma = pharma.drop_duplicates(subset="Name", keep="first").drop(columns="_tp")
    pharma.reset_index(drop=True, inplace=True)

    def _company_agg(grp):
        best_phase = _highest_phase(grp["Highest_Phase"])
        has_marketed = "Yes" if (grp["Market_Status"] == "Marketed").any() else "No"

        ta_set: set[str] = set()
        for ta_str in grp["Therapeutic_Area"].dropna():
            for ta in str(ta_str).split(","):
                ta = ta.strip()
                if ta and ta != "Unclassified":
                    ta_set.add(_normalize_ta(ta))
        if not ta_set:
            ta_set.add("Unclassified")

        all_ncts: set[str] = set()
        for ncts_str in grp["Trial_NCTIds"].dropna():
            for nct in str(ncts_str).split(","):
                nct = nct.strip()
                if nct:
                    all_ncts.add(nct)

        catalyst = ""
        for cat_str in grp["Next_Catalyst_Date"].dropna():
            dt = _parse_date(str(cat_str))
            if dt and dt >= TODAY:
                if not catalyst or dt < _parse_date(catalyst):
                    catalyst = str(cat_str)
        if not catalyst:
            has_passed = any(str(v).strip() == "Passed" for v in grp["Next_Catalyst_Date"].dropna())
            catalyst = "Passed" if has_passed else ""

        return pd.Series({
            "Highest_Phase": best_phase,
            "Has_Marketed_Drug": has_marketed,
            "Pipeline_Count": len(grp),
            "Total_Active_Trials": grp["Active_Trial_Count"].sum(),
            "Next_Catalyst": catalyst,
            "Therapeutic_Areas": ", ".join(sorted(ta_set)),
            "All_NCTIds": ", ".join(sorted(all_ncts)),
        })

    company_agg = pipeline.groupby("Symbol", as_index=False).apply(
        _company_agg, include_groups=False
    ).reset_index()

    merged = pharma.merge(company_agg, on="Symbol", how="left")

    merged["Highest_Phase"] = merged["Highest_Phase"].fillna("No Trials")
    merged["Has_Marketed_Drug"] = merged["Has_Marketed_Drug"].fillna("No")
    merged["Pipeline_Count"] = merged["Pipeline_Count"].fillna(0).astype(int)
    merged["Total_Active_Trials"] = merged["Total_Active_Trials"].fillna(0).astype(int)
    merged["Therapeutic_Areas"] = merged["Therapeutic_Areas"].fillna("")
    merged["All_NCTIds"] = merged["All_NCTIds"].fillna("")
    merged["Next_Catalyst"] = merged["Next_Catalyst"].fillna("")

    rows = []
    for _, row in merged.iterrows():
        ta_list = [t.strip() for t in row["Therapeutic_Areas"].split(",") if t.strip()]
        if not ta_list:
            ta_list = ["No Trials"]
        ta_list = list(dict.fromkeys(_normalize_ta(ta) for ta in ta_list))  # 去重并规范
        for ta in ta_list:
            new_row = row.copy()
            new_row["Therapeutic_Area_Filter"] = ta
            rows.append(new_row)

    exploded = pd.DataFrame(rows)

    orig_cols = list(pharma.columns)
    new_cols = [
        "Therapeutic_Area_Filter", "Therapeutic_Areas",
        "Highest_Phase", "Has_Marketed_Drug",
        "Pipeline_Count", "Total_Active_Trials",
        "Next_Catalyst", "All_NCTIds",
    ]
    insert_after = "Name"
    idx = orig_cols.index(insert_after) + 1
    final_order = orig_cols[:idx] + new_cols + orig_cols[idx:]
    exploded = exploded[final_order]

    phase_sort = exploded["Highest_Phase"].map(lambda x: PHASE_RANK.get(x, -1))
    exploded = exploded.assign(_pr=phase_sort).sort_values(
        ["Therapeutic_Area_Filter", "_pr", "Market Cap"],
        ascending=[True, False, False],
    ).drop(columns="_pr")

    exploded.to_csv(COMPANY_SUMMARY_CSV, index=False, encoding="utf-8-sig")

    n_companies = exploded["Symbol"].nunique()
    n_rows = len(exploded)
    n_ta = exploded["Therapeutic_Area_Filter"].nunique()
    print(f"  Company_Pipeline_Summary.csv: {n_rows} rows ({n_companies} companies x {n_ta} TAs)")
    print(f"  已保存 → {COMPANY_SUMMARY_CSV.name}")


# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════

def main():
    df = pd.read_csv(INPUT_CSV, dtype=str)
    print(f"读取 {len(df)} 条试验记录\n")

    df = df[
        df["Interventions"].notna()
        & (df["Interventions"] != "N/A")
        & (df["Interventions"].str.strip() != "")
        & df["Conditions"].notna()
        & (df["Conditions"] != "N/A")
    ].copy()
    print(f"过滤后保留 {len(df)} 条记录（有药物 + 有适应症）\n")

    # ── Explode: one row per asset per trial ───────────────────

    records = []
    for _, row in df.iterrows():
        assets = extract_assets(row["Interventions"])
        if not assets:
            continue
        for asset in assets:
            # 用归一化名作为 Asset_Name，使 Blinded ESK-001 / Open-Label ESK-001 / ESK-001 合并为一条
            canonical = normalize_asset_for_grouping(asset)
            if not canonical:
                continue
            records.append({
                "Symbol": row["Symbol"],
                "Company_Name": row["Company_Name"],
                "Asset_Name": canonical,
                "NCTId": row["NCTId"],
                "Phases": row.get("Phases", "N/A"),
                "Conditions": row.get("Conditions", "N/A"),
                "EnrollmentCount": row.get("EnrollmentCount", "N/A"),
                "PrimaryCompletionDate": row.get("PrimaryCompletionDate", "N/A"),
                "Status": row.get("Status", "N/A"),
            })

    exploded = pd.DataFrame(records)
    print(f"展开为 {len(exploded)} 条 (Asset x Trial) 记录\n")

    # ── Group by asset ────────────────────────────────────────

    def _agg(grp):
        all_conditions: list[str] = []
        for c in grp["Conditions"].dropna():
            for item in str(c).split(", "):
                item = item.strip()
                if item and item != "N/A":
                    all_conditions.append(item)
        unique_conditions = list(dict.fromkeys(all_conditions))

        enrollments = pd.to_numeric(grp["EnrollmentCount"], errors="coerce")

        return pd.Series({
            "Highest_Phase": _highest_phase(grp["Phases"]),
            "Active_Trial_Count": grp["NCTId"].nunique(),
            "Total_Enrollment": int(enrollments.sum()) if enrollments.notna().any() else 0,
            "Next_Catalyst_Date": _next_catalyst(grp["PrimaryCompletionDate"]),
            "Detailed_Conditions": "; ".join(unique_conditions),
            "Trial_NCTIds": ", ".join(grp["NCTId"].unique()),
        })

    grouped = exploded.groupby(
        ["Symbol", "Company_Name", "Asset_Name"], as_index=False
    ).apply(_agg, include_groups=False).reset_index()

    # 过滤掉漏网的 placebo/control 类 asset（如 MATCHING PLACEBO、Placebo Matching X 等）
    grouped = grouped[~grouped["Asset_Name"].apply(lambda x: is_placebo_asset(x) if pd.notna(x) else False)]

    print(f"聚合后 {len(grouped)} 条药物管线\n")

    # ── Phase sort ─────────────────────────────────────────────

    grouped["_phase_rank"] = grouped["Highest_Phase"].map(
        lambda x: PHASE_RANK.get(x, -1)
    )
    grouped.sort_values(
        ["_phase_rank", "Active_Trial_Count"],
        ascending=[False, False],
        inplace=True,
    )

    # ── Gemini: TA + MoA (combined batch call) ─────────────────

    print("调用 Gemini 进行 TA 归类 + MoA 推测（批量模式）...\n")

    ai_inputs: list[str] = []
    for _, row in grouped.iterrows():
        cond_short = str(row["Detailed_Conditions"])[:150]
        ai_inputs.append(f"{row['Asset_Name']} | {cond_short}")

    all_tas: list[str] = []
    all_moas: list[str] = []

    for i in tqdm(range(0, len(ai_inputs), BATCH_SIZE), desc="TA+MoA", unit="批"):
        batch = ai_inputs[i : i + BATCH_SIZE]
        batch_tas, batch_moas = classify_ta_moa_batch(batch)
        all_tas.extend(batch_tas)
        all_moas.extend(batch_moas)
        time.sleep(0.3)

    grouped["Therapeutic_Area"] = all_tas
    grouped["Mechanism_of_Action"] = all_moas

    # ── Gemini: Marketed / Investigational ─────────────────────

    print("\n调用 Gemini 判断药物上市状态（批量模式）...\n")
    drug_info_for_ai: list[str] = []
    for _, row in grouped.iterrows():
        conditions_short = str(row["Detailed_Conditions"])[:120]
        drug_info_for_ai.append(
            f"{row['Asset_Name']} | {row['Highest_Phase']} | {conditions_short}"
        )

    marketed_results: list[str] = []
    for i in tqdm(range(0, len(drug_info_for_ai), BATCH_SIZE), desc="上市判断", unit="批"):
        batch = drug_info_for_ai[i : i + BATCH_SIZE]
        batch_res = classify_marketed_batch(batch)
        marketed_results.extend(batch_res)
        time.sleep(0.3)

    grouped["Market_Status"] = marketed_results

    # ── Final output ──────────────────────────────────────────

    final_cols = [
        "Symbol", "Company_Name", "Asset_Name",
        "Highest_Phase", "Market_Status",
        "Active_Trial_Count", "Next_Catalyst_Date",
        "Mechanism_of_Action",
        "Therapeutic_Area", "Detailed_Conditions", "Trial_NCTIds",
    ]
    result = grouped[final_cols]

    # TA 规范化后写入 Master，保证与 Summary 一致
    def _norm_ta_cell(v):
        if pd.isna(v) or not str(v).strip():
            return v
        parts = [_normalize_ta(x.strip()) for x in str(v).split(",") if x.strip()]
        return ", ".join(parts) if parts else v

    result["Therapeutic_Area"] = result["Therapeutic_Area"].apply(_norm_ta_cell)
    result.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    # ── Company-level summary ─────────────────────────────────

    print("\n生成公司级汇总并合并到公司名单...\n")
    _build_company_summary(result)

    # ── Stats ─────────────────────────────────────────────────

    print("\n" + "=" * 65)
    print(f"  总药物管线数           {len(result)}")
    print(f"  涉及公司数             {result['Symbol'].nunique()}")
    print(f"\n  上市状态分布:")
    for status, cnt in result["Market_Status"].value_counts().items():
        print(f"    {status:<20} {cnt}")
    print(f"\n  Phase 分布:")
    for phase, cnt in result["Highest_Phase"].value_counts().items():
        print(f"    {phase:<20} {cnt}")
    print(f"\n  催化剂时间分布:")
    has_future = (result["Next_Catalyst_Date"] != "") & (result["Next_Catalyst_Date"] != "Passed")
    passed = result["Next_Catalyst_Date"] == "Passed"
    empty = result["Next_Catalyst_Date"] == ""
    print(f"    有未来催化剂日期     {has_future.sum()}")
    print(f"    已过期 (Passed)      {passed.sum()}")
    print(f"    无数据               {empty.sum()}")
    print(f"\n  MoA 分布 (Top 15):")
    moa_vals = result["Mechanism_of_Action"].value_counts()
    for moa, cnt in moa_vals.head(15).items():
        print(f"    {moa:<35} {cnt}")
    print(f"\n  TA 分布 (Top 10):")
    ta_flat = result["Therapeutic_Area"].str.split(r",\s*").explode()
    for ta, cnt in ta_flat.value_counts().head(10).items():
        print(f"    {ta:<30} {cnt}")
    print("=" * 65)
    print(f"\n已保存 → {OUTPUT_CSV.name}")


if __name__ == "__main__":
    main()
