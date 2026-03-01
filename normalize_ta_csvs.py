"""
一次性规范化 Therapeutic Area：在现有 CSV 上应用与 build_pipeline 一致的 TA 映射。
- 合并/改名规则同 build_pipeline
- Reproductive Health, Rheumatology, Unclassified 及未在固定列表中的 → Others
- 固定列表为准，不新增 TA
"""

import pathlib
import sys

import pandas as pd

# 与 build_pipeline 一致的固定 TA 列表（不再新增）
CANONICAL_TA = frozenset([
    "Cardiovascular", "Dermatology", "Gastroenterology", "Hematology",
    "Immunology", "Infectious Diseases", "Metabolic/Endocrinology", "Musculoskeletal",
    "Neurology/CNS", "No Trials", "Ophthalmology", "Others", "Pain", "Rare Disease",
    "Respiratory", "Urology/Nephrology",
])


def normalize_ta(ta: str) -> str:
    if not ta or not str(ta).strip():
        return ""
    t = str(ta).strip()
    if "parkinson" in t.lower() or t == "Psychiatry":
        return "Neurology/CNS"
    if "rare" in t.lower() and ("orphan" in t.lower() or "/" in t):
        return "Rare Disease"
    if t == "Rare/Orphan Diseases":
        return "Rare Disease"
    if "pain" in t.lower() or "analgesia" in t.lower():
        return "Pain"
    if t == "Endocrinology" or t == "Endocrinology/Metabolic":
        return "Metabolic/Endocrinology"
    if t == "Immunology/Autoimmune":
        return "Immunology"
    if t not in CANONICAL_TA:
        return "Others"
    return t


def main() -> int:
    data_dir = pathlib.Path(__file__).parent

    # 1) Company_Pipeline_Summary.csv
    summary_path = data_dir / "Company_Pipeline_Summary.csv"
    if not summary_path.exists():
        print(f"  Skip: {summary_path.name} not found.")
    else:
        df = pd.read_csv(summary_path, encoding="utf-8-sig")
        if "Therapeutic_Area_Filter" in df.columns:
            df["Therapeutic_Area_Filter"] = df["Therapeutic_Area_Filter"].apply(
                lambda x: normalize_ta(str(x)) if pd.notna(x) else x
            )
        if "Therapeutic_Areas" in df.columns:
            def norm_areas(s):
                if pd.isna(s) or not str(s).strip():
                    return s
                parts = [normalize_ta(x.strip()) for x in str(s).split(",") if x.strip()]
                seen = set()
                unique = []
                for p in parts:
                    if p not in seen:
                        seen.add(p)
                        unique.append(p)
                return ", ".join(unique)
            df["Therapeutic_Areas"] = df["Therapeutic_Areas"].apply(norm_areas)
        df.to_csv(summary_path, index=False, encoding="utf-8-sig")
        print(f"  Updated {summary_path.name}")

    # 2) Biotech_Pipeline_Master.csv
    master_path = data_dir / "Biotech_Pipeline_Master.csv"
    if not master_path.exists():
        print(f"  Skip: {master_path.name} not found.")
    else:
        df = pd.read_csv(master_path, encoding="utf-8-sig")
        if "Therapeutic_Area" in df.columns:
            def norm_ta_cell(v):
                if pd.isna(v) or not str(v).strip():
                    return v
                parts = [normalize_ta(x.strip()) for x in str(v).split(",") if x.strip()]
                return ", ".join(parts) if parts else v
            df["Therapeutic_Area"] = df["Therapeutic_Area"].apply(norm_ta_cell)
        df.to_csv(master_path, index=False, encoding="utf-8-sig")
        print(f"  Updated {master_path.name}")

    print("Done. TA normalization applied to source CSVs.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
