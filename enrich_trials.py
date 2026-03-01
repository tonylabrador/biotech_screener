"""
Enrich Raw_Clinical_Trials.csv with detailed fields from CTG API v2
───────────────────────────────────────────────────────────────────
Uses existing NCTId list to batch-fetch additional study metadata.
Does NOT re-query by sponsor — goes directly by NCTId for precision.

Input  : Raw_Clinical_Trials.csv
Output : Enriched_Clinical_Trials.csv
"""

import io
import sys
import time
import pathlib
import pandas as pd
import requests
from tqdm import tqdm

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace"
    )
    sys.stderr = io.TextIOWrapper(
        sys.stderr.buffer, encoding="utf-8", errors="replace"
    )

DATA_DIR = pathlib.Path(__file__).parent
INPUT_CSV = DATA_DIR / "Raw_Clinical_Trials.csv"
OUTPUT_CSV = DATA_DIR / "Enriched_Clinical_Trials.csv"
CHECKPOINT_CSV = DATA_DIR / "Enriched_Checkpoint.csv"

CTG_STUDY_URL = "https://clinicaltrials.gov/api/v2/studies/{nct_id}"

# ── Field extraction ──────────────────────────────────────────

def extract_fields(data: dict) -> dict:
    proto = data.get("protocolSection", {})
    ident = proto.get("identificationModule", {})
    status_mod = proto.get("statusModule", {})
    design = proto.get("designModule", {})
    desc = proto.get("descriptionModule", {})
    elig = proto.get("eligibilityModule", {})
    arms_mod = proto.get("armsInterventionsModule", {})
    sponsor_mod = proto.get("sponsorCollaboratorsModule", {})
    outcomes_mod = proto.get("outcomesModule", {})

    # Design info
    design_info = design.get("designInfo", {})
    masking_info = design_info.get("maskingInfo", {})
    enrollment = design.get("enrollmentInfo", {})

    # Dates
    start = status_mod.get("startDateStruct", {})
    primary_comp = status_mod.get("primaryCompletionDateStruct", {})
    completion = status_mod.get("completionDateStruct", {})

    # Arms
    arm_groups = arms_mod.get("armGroups", [])
    arm_labels = [a.get("label", "") for a in arm_groups]
    arm_types = [a.get("type", "") for a in arm_groups]

    # Primary outcomes
    primary_outcomes = outcomes_mod.get("primaryOutcomes", [])
    primary_measures = [o.get("measure", "") for o in primary_outcomes if o.get("measure")]

    # Collaborators
    collabs = sponsor_mod.get("collaborators", [])
    collab_names = [c.get("name", "") for c in collabs if c.get("name")]

    return {
        "OfficialTitle": ident.get("officialTitle", "N/A"),
        "Acronym": ident.get("acronym", "N/A"),
        "BriefSummary": (desc.get("briefSummary", "") or "")[:500],
        "StudyType": design.get("studyType", "N/A"),
        "Allocation": design_info.get("allocation", "N/A"),
        "InterventionModel": design_info.get("interventionModel", "N/A"),
        "PrimaryPurpose": design_info.get("primaryPurpose", "N/A"),
        "Masking": masking_info.get("masking", "N/A"),
        "EnrollmentCount": enrollment.get("count", "N/A"),
        "EnrollmentType": enrollment.get("type", "N/A"),
        "ArmCount": len(arm_groups),
        "ArmLabels": " | ".join(arm_labels) if arm_labels else "N/A",
        "ArmTypes": " | ".join(arm_types) if arm_types else "N/A",
        "StartDate": start.get("date", "N/A"),
        "StartDateType": start.get("type", "N/A"),
        "PrimaryCompletionDate": primary_comp.get("date", "N/A"),
        "PrimaryCompletionDateType": primary_comp.get("type", "N/A"),
        "CompletionDate": completion.get("date", "N/A"),
        "CompletionDateType": completion.get("type", "N/A"),
        "PrimaryOutcomeMeasures": " | ".join(primary_measures) if primary_measures else "N/A",
        "EligibleAges": f"{elig.get('minimumAge', 'N/A')} - {elig.get('maximumAge', 'N/A')}",
        "Sex": elig.get("sex", "N/A"),
        "LeadSponsorClass": sponsor_mod.get("leadSponsor", {}).get("class", "N/A"),
        "Collaborators": ", ".join(collab_names) if collab_names else "N/A",
    }


# ── Main ──────────────────────────────────────────────────────

def main():
    df = pd.read_csv(INPUT_CSV)
    print(f"原始试验记录: {len(df)} 条，唯一 NCTId: {df['NCTId'].nunique()} 个\n")

    unique_ncts = df["NCTId"].dropna().unique().tolist()
    unique_ncts = [n for n in unique_ncts if n != "N/A"]

    # Load checkpoint if exists
    already_done = set()
    enriched_cache = {}
    if CHECKPOINT_CSV.exists():
        ckpt = pd.read_csv(CHECKPOINT_CSV)
        already_done = set(ckpt["NCTId"].tolist())
        for _, row in ckpt.iterrows():
            enriched_cache[row["NCTId"]] = row.to_dict()
        print(f"从 checkpoint 恢复 {len(already_done)} 条已完成记录\n")

    to_fetch = [n for n in unique_ncts if n not in already_done]
    print(f"需要查询: {len(to_fetch)} 个 NCTId\n")

    batch_rows = []
    errors = 0

    for nct_id in tqdm(to_fetch, desc="NCTId 详情抓取", unit="条"):
        url = CTG_STUDY_URL.format(nct_id=nct_id)
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            fields = extract_fields(data)
            fields["NCTId"] = nct_id
            enriched_cache[nct_id] = fields
            batch_rows.append(fields)
        except Exception:
            errors += 1
            enriched_cache[nct_id] = {"NCTId": nct_id}
            batch_rows.append({"NCTId": nct_id})

        # Checkpoint every 100 records
        if len(batch_rows) % 100 == 0 and batch_rows:
            _save_checkpoint(enriched_cache)

        time.sleep(0.2)

    # Final checkpoint
    if batch_rows:
        _save_checkpoint(enriched_cache)

    # Merge enriched data back into original DataFrame
    enrich_df = pd.DataFrame(list(enriched_cache.values()))
    merged = df.merge(enrich_df, on="NCTId", how="left")

    # Reorder columns: original first, then new
    orig_cols = list(df.columns)
    new_cols = [c for c in merged.columns if c not in orig_cols]
    merged = merged[orig_cols + sorted(new_cols)]

    merged.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    # Cleanup checkpoint
    if CHECKPOINT_CSV.exists():
        CHECKPOINT_CSV.unlink()

    print("\n" + "=" * 60)
    print(f"  原始记录数            {len(df)} 条")
    print(f"  唯一 NCTId            {len(unique_ncts)} 个")
    print(f"  本次新查询            {len(to_fetch)} 个")
    print(f"  查询失败              {errors} 个")
    print(f"  最终列数              {len(merged.columns)} 列")
    print(f"  新增字段              {len(new_cols)} 个")
    print("=" * 60)
    print(f"\n新增字段: {new_cols}")
    print(f"\n已保存 -> {OUTPUT_CSV.name}")


def _save_checkpoint(cache: dict):
    rows = list(cache.values())
    pd.DataFrame(rows).to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()
