"""
═══════════════════════════════════════════════════════════════
  Biotech Pipeline — 一键全流程执行器
═══════════════════════════════════════════════════════════════

Usage:
    python run_pipeline.py                  # 全量重跑（从 xlsx 开始）
    python run_pipeline.py --from step2     # 从 step2 开始（跳过 yfinance）
    python run_pipeline.py --from step3     # 从 step3 开始（跳过试验抓取）
    python run_pipeline.py --from step4     # 仅重跑 enrich + pipeline

Pipeline Steps:
    step1  clean_biotech.py     合并 xlsx → yfinance 补充 → 过滤 → Final_Non_Oncology_Pharma.csv
    step2  fetch_trials.py      CTG API + Gemini → Raw_Clinical_Trials.csv
    step3  enrich_trials.py     NCTId 详情补全 → Enriched_Clinical_Trials.csv
    step4  build_pipeline.py    聚合 + Gemini TA 归类 → Biotech_Pipeline_Master.csv
    step5  post_process         Oncology 二次筛查 + 无试验公司列表

Prerequisites:
    - .env 文件包含 GEMINI_API_KEY
    - xlsx 数据文件放在当前目录
    - pip install -r requirements.txt
═══════════════════════════════════════════════════════════════
"""

import io
import subprocess
import sys
import time
import pathlib
import argparse

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace"
    )
    sys.stderr = io.TextIOWrapper(
        sys.stderr.buffer, encoding="utf-8", errors="replace"
    )

DATA_DIR = pathlib.Path(__file__).parent
PYTHON = sys.executable

STEPS = {
    "step1": ("clean_biotech.py", "Step 1/5: 合并 XLSX + yfinance 补充 + 双重过滤"),
    "step2": ("fetch_trials.py", "Step 2/5: CTG API 试验抓取 (三重防线 + Gemini)"),
    "step3": ("enrich_trials.py", "Step 3/5: NCTId 详情补全"),
    "step4": ("build_pipeline.py", "Step 4/5: 管线聚合 + Gemini TA 归类"),
    "step5": (None, "Step 5/5: Oncology 二次筛查 + 收尾"),
}

STEP_ORDER = ["step1", "step2", "step3", "step4", "step5"]

# Oncology-heavy companies to always remove (updated after each full run)
ONCOLOGY_HEAVY_SYMBOLS = {
    "AAVXF", "ABVX", "ACOG", "BCAX", "BMEA", "BMYMP",
    "CHSYF", "CSWYF", "CSWYY", "GTBIF", "KLRS", "KRYS",
    "KYNB", "LJUIF", "MDCLF", "NNBXF", "PFE", "PLRX",
    "SABS", "SGBCF", "SHJBF", "SXPHF", "ULIHF",
}


def run_script(script_name: str, description: str) -> bool:
    print()
    print("=" * 65)
    print(f"  {description}")
    print("=" * 65)
    print()

    script_path = DATA_DIR / script_name
    if not script_path.exists():
        print(f"  ERROR: {script_name} not found!")
        return False

    start = time.time()
    result = subprocess.run(
        [PYTHON, str(script_path)],
        cwd=str(DATA_DIR),
        env=None,
    )
    elapsed = time.time() - start
    mins, secs = divmod(int(elapsed), 60)

    if result.returncode != 0:
        print(f"\n  FAILED (exit code {result.returncode}) after {mins}m {secs}s")
        return False

    print(f"\n  Completed in {mins}m {secs}s")
    return True


def _ticker_priority(sym: str) -> int:
    """Lower = better. US primary > ADR (Y) > OTC foreign (F) > others."""
    s = str(sym).strip()
    if ("." in s) or (":" in s):
        return 3
    if len(s) >= 4 and s[-1] == "F":
        return 2
    if len(s) >= 4 and s[-1] == "Y":
        return 1
    return 0


def _dedup_tickers():
    """
    Deduplicate: same company listed under multiple tickers.
    Keep the highest-priority ticker (US primary > ADR > OTC foreign).
    In downstream files (trials, pipeline), remap dropped tickers to the kept one.
    """
    import pandas as pd

    pharma_path = DATA_DIR / "Final_Non_Oncology_Pharma.csv"
    if not pharma_path.exists():
        return

    pharma = pd.read_csv(pharma_path)
    pharma["_tp"] = pharma["Symbol"].apply(_ticker_priority)
    pharma.sort_values("_tp", inplace=True)

    dupe_mask = pharma.duplicated(subset="Name", keep="first")
    dupes = pharma[dupe_mask]
    if len(dupes) == 0:
        pharma.drop(columns="_tp", inplace=True)
        print("  Ticker dedup: no duplicates found")
        return

    # Build remap dict: dropped_symbol -> kept_symbol
    remap = {}
    for _, dup_row in dupes.iterrows():
        kept = pharma[(pharma["Name"] == dup_row["Name"]) & ~dupe_mask]
        if len(kept) > 0:
            remap[dup_row["Symbol"]] = kept.iloc[0]["Symbol"]

    print(f"  Ticker dedup: {len(remap)} duplicate tickers -> remapping")
    for old, new in sorted(remap.items()):
        print(f"    {old:>8} -> {new}")

    pharma = pharma[~dupe_mask].drop(columns="_tp")
    pharma.reset_index(drop=True, inplace=True)
    pharma.to_csv(pharma_path, index=False, encoding="utf-8-sig")
    print(f"  Final_Non_Oncology_Pharma.csv: {len(pharma)} rows after dedup")

    # Remap in downstream trial/pipeline files
    for fname in [
        "Raw_Clinical_Trials.csv",
        "Enriched_Clinical_Trials.csv",
        "Biotech_Pipeline_Master.csv",
        "Companies_No_Active_Trials.csv",
        "Company_Pipeline_Summary.csv",
    ]:
        fpath = DATA_DIR / fname
        if not fpath.exists():
            continue
        df = pd.read_csv(fpath)
        if "Symbol" not in df.columns:
            continue
        before = len(df)
        df["Symbol"] = df["Symbol"].replace(remap)
        if fname == "Company_Pipeline_Summary.csv" and "Therapeutic_Area_Filter" in df.columns:
            df = df.drop_duplicates(subset=["Symbol", "Therapeutic_Area_Filter"], keep="first")
        else:
            df.drop_duplicates(inplace=True)
        df.to_csv(fpath, index=False, encoding="utf-8-sig")
        print(f"  {fname}: remapped, {before} -> {len(df)} rows")


def post_process():
    """Step 5: Ticker dedup + Remove oncology-heavy companies + summaries."""
    import pandas as pd

    print()
    print("=" * 65)
    print("  Step 5/5: Ticker 去重 + Oncology 二次筛查 + 收尾")
    print("=" * 65)
    print()

    # --- 5a: Ticker dedup ---
    _dedup_tickers()

    # --- 5a2: Company_Pipeline_Summary: one row per (Symbol, TA) ---
    summary_path = DATA_DIR / "Company_Pipeline_Summary.csv"
    if summary_path.exists():
        sdf = pd.read_csv(summary_path)
        if "Therapeutic_Area_Filter" in sdf.columns:
            before_s = len(sdf)
            sdf = sdf.drop_duplicates(subset=["Symbol", "Therapeutic_Area_Filter"], keep="first")
            if len(sdf) < before_s:
                sdf.to_csv(summary_path, index=False, encoding="utf-8-sig")
                print(f"  Company_Pipeline_Summary.csv: {before_s} -> {len(sdf)} rows (dropped duplicate Symbol+TA)")
    print()

    # --- 5b: Oncology scan ---
    pipeline_path = DATA_DIR / "Biotech_Pipeline_Master.csv"
    if not pipeline_path.exists():
        print("  WARNING: Biotech_Pipeline_Master.csv not found, skipping oncology scan")
        return

    pipeline = pd.read_csv(pipeline_path)

    # Identify companies with high oncology ratio
    onc_mask = pipeline["Therapeutic_Area"].str.contains("Oncology", case=False, na=False)
    onc_rows = pipeline[onc_mask]

    if len(onc_rows) > 0:
        onc_rows.to_csv(
            DATA_DIR / "Oncology_Pipelines.csv",
            index=False, encoding="utf-8-sig",
        )
        print(f"  Oncology pipeline rows found: {len(onc_rows)}")
        print(f"  Saved -> Oncology_Pipelines.csv")

        # Determine which companies are oncology-heavy
        company_stats = pipeline.groupby("Symbol").agg(
            total=("Asset_Name", "count"),
            onc_count=("Therapeutic_Area", lambda x: x.str.contains("Oncology", case=False, na=False).sum()),
        )
        company_stats["ratio"] = company_stats["onc_count"] / company_stats["total"]
        heavy = company_stats[
            (company_stats["ratio"] > 0.30) | (company_stats["onc_count"] >= 10)
        ].index.tolist()

        all_remove = ONCOLOGY_HEAVY_SYMBOLS | set(heavy)

        if all_remove:
            print(f"\n  Removing {len(all_remove)} oncology-heavy companies from all files...")

            for fname in [
                "Final_Non_Oncology_Pharma.csv",
                "Raw_Clinical_Trials.csv",
                "Enriched_Clinical_Trials.csv",
                "Companies_No_Active_Trials.csv",
            ]:
                fpath = DATA_DIR / fname
                if fpath.exists():
                    df = pd.read_csv(fpath)
                    before = len(df)
                    df = df[~df["Symbol"].isin(all_remove)]
                    df.to_csv(fpath, index=False, encoding="utf-8-sig")
                    print(f"    {fname:<40} {before} -> {len(df)}")

            # Pipeline: remove companies + oncology rows for kept companies
            before = len(pipeline)
            pipeline = pipeline[~pipeline["Symbol"].isin(all_remove)]
            pipeline = pipeline[
                ~pipeline["Therapeutic_Area"].str.contains("Oncology", case=False, na=False)
            ]
            pipeline.to_csv(pipeline_path, index=False, encoding="utf-8-sig")
            print(f"    {'Biotech_Pipeline_Master.csv':<40} {before} -> {len(pipeline)}")
    else:
        print("  No oncology pipelines found.")

    # --- 5c: No-trials list ---
    pharma_path = DATA_DIR / "Final_Non_Oncology_Pharma.csv"
    trials_path = DATA_DIR / "Raw_Clinical_Trials.csv"

    if pharma_path.exists() and trials_path.exists():
        pharma = pd.read_csv(pharma_path)
        trials = pd.read_csv(trials_path)
        trial_syms = set(trials["Symbol"].unique())
        no_trial = pharma[~pharma["Symbol"].isin(trial_syms)][
            ["Symbol", "Name", "Market Cap", "Industry"]
        ].copy()
        no_trial.sort_values("Market Cap", ascending=False, inplace=True)
        no_trial.to_csv(
            DATA_DIR / "Companies_No_Active_Trials.csv",
            index=False, encoding="utf-8-sig",
        )
        print(f"\n  Companies with no active trials: {len(no_trial)}")
        print(f"  Saved -> Companies_No_Active_Trials.csv")

    # --- 5d: Summary ---
    print()
    print("=" * 65)
    print("  PIPELINE COMPLETE — Summary")
    print("=" * 65)

    for fname, desc in [
        ("Final_Non_Oncology_Pharma.csv", "Non-onc companies"),
        ("Raw_Clinical_Trials.csv", "Raw trials"),
        ("Enriched_Clinical_Trials.csv", "Enriched trials"),
        ("Biotech_Pipeline_Master.csv", "Asset pipelines"),
        ("Companies_No_Active_Trials.csv", "No-trial companies"),
        ("Oncology_Pipelines.csv", "Oncology pipelines (ref)"),
    ]:
        fpath = DATA_DIR / fname
        if fpath.exists():
            df = pd.read_csv(fpath)
            print(f"    {desc:<30} {len(df):>6} rows  <- {fname}")

    print("=" * 65)


def main():
    parser = argparse.ArgumentParser(description="Biotech Pipeline Runner")
    parser.add_argument(
        "--from", dest="start_from", default="step1",
        choices=STEP_ORDER,
        help="Start from a specific step (default: step1)",
    )
    args = parser.parse_args()

    start_idx = STEP_ORDER.index(args.start_from)
    steps_to_run = STEP_ORDER[start_idx:]

    print()
    print("╔" + "═" * 63 + "╗")
    print("║   Biotech Non-Oncology Pipeline — Full Run                    ║")
    print("╚" + "═" * 63 + "╝")
    print(f"\n  Starting from: {args.start_from}")
    print(f"  Steps to run:  {' -> '.join(steps_to_run)}")

    total_start = time.time()

    for step_key in steps_to_run:
        script, desc = STEPS[step_key]

        if step_key == "step5":
            post_process()
        else:
            ok = run_script(script, desc)
            if not ok:
                print(f"\n  Pipeline ABORTED at {step_key}.")
                sys.exit(1)

    total_elapsed = time.time() - total_start
    mins, secs = divmod(int(total_elapsed), 60)
    print(f"\n  Total pipeline time: {mins}m {secs}s")


if __name__ == "__main__":
    main()
