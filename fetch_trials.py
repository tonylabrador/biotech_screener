"""
Batch-fetch active clinical trials from ClinicalTrials.gov API v2
─────────────────────────────────────────────────────────────────
Three-tier search strategy:
  1st  Regex-cleaned name variants
  2nd  Gemini AI sponsor alias resolution
  3rd  First-word fallback

Input  : Final_Non_Oncology_Pharma.csv  (Symbol + Name)
Output : Raw_Clinical_Trials.csv
"""

import io
import json
import os
import re
import sys
import time
import pathlib
import pandas as pd
import requests
from tqdm import tqdm
from dotenv import load_dotenv
from google import genai

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace"
    )
    sys.stderr = io.TextIOWrapper(
        sys.stderr.buffer, encoding="utf-8", errors="replace"
    )

# ── Paths ─────────────────────────────────────────────────────

DATA_DIR = pathlib.Path(__file__).parent
INPUT_CSV = DATA_DIR / "Final_Non_Oncology_Pharma.csv"
OUTPUT_CSV = DATA_DIR / "Raw_Clinical_Trials.csv"

# ── Gemini init ───────────────────────────────────────────────

load_dotenv(DATA_DIR / ".env")

_GEMINI_KEY = os.environ.get("GEMINI_API_KEY", "")
_gemini_client = genai.Client(api_key=_GEMINI_KEY) if _GEMINI_KEY else None

_GEMINI_PROMPT = (
    "你是一个专业的医疗投资分析师。"
    "用户会给你一个上市生物科技公司的财务注册名称。"
    "请你输出该公司在 ClinicalTrials.gov 上最可能使用的 1 到 3 个"
    "主要申办方（Sponsor）或核心子公司的英文名称。"
    "例如 Roivant Sciences Ltd. 应该输出 Immunovant, Dermavant, Priovant。"
    "请务必只返回一个标准的 JSON 格式的字符串数组，"
    '例如：["Sponsor A", "Sponsor B"]。'
    "绝对不要包含任何 Markdown 标记（如 ```json）、不要任何解释。"
)

# ── CTG constants ─────────────────────────────────────────────

CTG_BASE = "https://clinicaltrials.gov/api/v2/studies"
ACTIVE_STATUSES = "RECRUITING,ACTIVE_NOT_RECRUITING,NOT_YET_RECRUITING"
PAGE_SIZE = 50

OUTPUT_COLUMNS = [
    "Symbol", "Company_Name", "NCTId", "Phases",
    "Status", "Conditions", "Interventions",
]

# ── Company name cleaner (Tier 1) ────────────────────────────

_LEGAL_SUFFIXES = re.compile(
    r"""\b(
        Inc\.?|Corp\.?|Corporation|Ltd\.?|Limited|LLC|PLC|plc|
        S\.?A\.?B?\.?\s*(de\s+C\.?\s*V\.?)?|
        S\.p\.A\.?|SpA|
        N\.V\.?|NV|SE|AG|GmbH|Co\.?,?\s*Ltd\.?|Co\.?|
        A/S|AS|Oyj|AB|d\.?\s?d\.?|
        de\s+C\.V\.?|B\.\s*de\s+C\.V\.?|
        Tbk|PT|Aktiengesellschaft|
        \(publ\)|\(Publ\)|
        Preference\s+Shares|PFD\s+CONV\s*\d*|
        WT\s+EXP\s+\w*\s*\d*|
        CONTINGENT\s+VAL\s+RIGHTS\s*[\d/]*
    )\b""",
    re.IGNORECASE | re.VERBOSE,
)

_BIZ_SUFFIXES = re.compile(
    r"""\b(
        Incorporated|Therapeutics|Pharmaceuticals|Pharmaceutical|Pharmacare|
        Biotherapeutics|Biopharmaceuticals|Biosciences|Bioscience|
        Biopharma|Biotech|Biotechnology|Labs|Laboratories|
        Holdings|Holding|Group|Company|
        Health|Healthcare|Sciences|Medical|
        International|Global|
        Animal\s+Health
    )\b""",
    re.IGNORECASE | re.VERBOSE,
)

_SPECIAL_PREFIXES = re.compile(r"^(The|PT)\s+", re.IGNORECASE)
_PARENTHETICAL = re.compile(r"\([^)]*\)")
_TRAILING_JUNK = re.compile(r"[,.\s/\-]+$")
_MULTI_SPACES = re.compile(r"\s{2,}")


def _do_clean(name: str, strip_biz: bool = True) -> str:
    cleaned = _PARENTHETICAL.sub("", name)
    cleaned = _LEGAL_SUFFIXES.sub("", cleaned)
    if strip_biz:
        cleaned = _BIZ_SUFFIXES.sub("", cleaned)
    cleaned = _SPECIAL_PREFIXES.sub("", cleaned)
    cleaned = _TRAILING_JUNK.sub("", cleaned)
    cleaned = _MULTI_SPACES.sub(" ", cleaned).strip()
    return cleaned


def generate_query_variants(name: str) -> list[str]:
    aggressive = _do_clean(name, strip_biz=True)
    conservative = _do_clean(name, strip_biz=False)

    candidates = []
    if conservative:
        candidates.append(conservative)
    if aggressive and aggressive != conservative:
        candidates.append(aggressive)

    words = aggressive.split() if aggressive else conservative.split()
    if len(words) >= 3:
        candidates.append(" ".join(words[:2]))

    cwords = conservative.split() if conservative else []
    if len(cwords) >= 4:
        candidates.append(" ".join(cwords[:2]))

    seen = set()
    result = []
    for v in candidates:
        v = _TRAILING_JUNK.sub("", v).strip()
        if v and len(v) > 2 and v not in seen:
            seen.add(v)
            result.append(v)
    return result if result else [name.strip()]


# ── Gemini sponsor alias resolver (Tier 2) ───────────────────

_MD_FENCE = re.compile(r"^```(?:json)?\s*\n?|\n?```\s*$", re.MULTILINE)


def get_sponsor_aliases(company_name: str) -> list[str]:
    if not _gemini_client:
        return []
    try:
        response = _gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=f"{_GEMINI_PROMPT}\n\n公司名称: {company_name}",
        )
        raw = _MD_FENCE.sub("", response.text).strip()
        aliases = json.loads(raw)
        if isinstance(aliases, list):
            return [a.strip() for a in aliases if isinstance(a, str) and a.strip()]
    except Exception:
        pass
    return []


# ── JSON parser for a single study ────────────────────────────

def parse_study(study: dict) -> dict:
    proto = study.get("protocolSection", {})
    ident = proto.get("identificationModule", {})
    design = proto.get("designModule", {})
    status_mod = proto.get("statusModule", {})
    cond_mod = proto.get("conditionsModule", {})
    arms_mod = proto.get("armsInterventionsModule", {})

    nct_id = ident.get("nctId", "N/A")

    phases_list = design.get("phases") or []
    phases = ", ".join(phases_list) if phases_list else "N/A"

    overall_status = status_mod.get("overallStatus", "N/A")

    conditions_list = cond_mod.get("conditions") or []
    conditions = ", ".join(conditions_list) if conditions_list else "N/A"

    interventions_raw = arms_mod.get("interventions") or []
    drug_names = [
        iv.get("name", "")
        for iv in interventions_raw
        if iv.get("type") in ("DRUG", "BIOLOGICAL")
    ]
    interventions = ", ".join(drug_names) if drug_names else "N/A"

    return {
        "NCTId": nct_id,
        "Phases": phases,
        "Status": overall_status,
        "Conditions": conditions,
        "Interventions": interventions,
    }


# ── Paginated fetcher for one sponsor ─────────────────────────

def fetch_trials_for_sponsor(sponsor_query: str) -> list[dict]:
    all_records = []
    page_token = None

    while True:
        params = {
            "query.spons": sponsor_query,
            "filter.overallStatus": ACTIVE_STATUSES,
            "pageSize": PAGE_SIZE,
        }
        if page_token:
            params["pageToken"] = page_token

        try:
            resp = requests.get(CTG_BASE, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except (requests.RequestException, ValueError):
            break

        studies = data.get("studies") or []
        for s in studies:
            all_records.append(parse_study(s))

        page_token = data.get("nextPageToken")
        if not page_token:
            break

        time.sleep(0.3)

    return all_records


# ── Veterinary company detector ───────────────────────────────

_VET_KEYWORDS = re.compile(
    r"\b(veterinary|animal health|livestock|pet\s+health|equine)\b",
    re.IGNORECASE,
)


def _is_vet_company(row: pd.Series) -> bool:
    for col in ("Industry", "Business Summary"):
        val = str(row.get(col, ""))
        if _VET_KEYWORDS.search(val):
            return True
    return False


# ── Main ──────────────────────────────────────────────────────

def main():
    df = pd.read_csv(INPUT_CSV)
    companies = df.drop_duplicates(subset="Symbol")
    print(f"共 {len(companies)} 家公司待查询")
    if _gemini_client:
        print("✦ Gemini AI 引擎已就绪 (gemini-2.5-flash)")
    else:
        print("⚠ 未检测到 GEMINI_API_KEY，将跳过 AI 破译层")
    print()

    header_df = pd.DataFrame(columns=OUTPUT_COLUMNS)
    header_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    total_trials = 0
    companies_with_trials = 0
    ai_assists = 0

    for _, row in tqdm(companies.iterrows(), total=len(companies),
                       desc="CTG 抓取", unit="家"):
        symbol = row["Symbol"]
        name = row["Name"]
        records = []
        matched_query = ""

        # ── Tier 1: Regex-cleaned variants ────────────────────
        variants = generate_query_variants(name)
        for query in variants:
            records = fetch_trials_for_sponsor(query)
            if records:
                matched_query = query
                break
            time.sleep(0.3)

        # ── Tier 2: Gemini AI alias resolution ────────────────
        if not records and _gemini_client and not _is_vet_company(row):
            aliases = get_sponsor_aliases(name)
            if aliases:
                tqdm.write(
                    f"  [AI 破译] {name} -> 识别为: {aliases}"
                )
                for alias in aliases:
                    records = fetch_trials_for_sponsor(alias)
                    if records:
                        matched_query = alias
                        ai_assists += 1
                        tqdm.write(
                            f"  [AI 破译] {name} -> "
                            f"query='{alias}' -> "
                            f"成功抓取 {len(records)} 个试验！"
                        )
                        break
                    time.sleep(0.3)

        # ── Tier 3: First-word fallback ───────────────────────
        if not records:
            first_word = _do_clean(name, strip_biz=True).split()[0] if _do_clean(name, strip_biz=True) else ""
            if first_word and len(first_word) > 2 and first_word not in {v for v in variants}:
                records = fetch_trials_for_sponsor(first_word)
                if records:
                    matched_query = first_word
                time.sleep(0.3)

        # ── Write results ─────────────────────────────────────
        if records:
            companies_with_trials += 1
            rows = []
            for rec in records:
                r = {"Symbol": symbol, "Company_Name": name}
                r.update(rec)
                rows.append(r)

            chunk = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
            chunk.to_csv(
                OUTPUT_CSV, mode="a", header=False,
                index=False, encoding="utf-8-sig",
            )
            total_trials += len(rows)

        time.sleep(0.3)

    print("\n" + "═" * 60)
    print(f"  查询公司总数            {len(companies)} 家")
    print(f"  有活跃试验的公司        {companies_with_trials} 家")
    print(f"  其中 AI 辅助匹配        {ai_assists} 家")
    print(f"  抓取到的活跃试验总数    {total_trials} 条")
    print("═" * 60)
    print(f"\n结果已保存 → {OUTPUT_CSV.name}")


if __name__ == "__main__":
    main()
