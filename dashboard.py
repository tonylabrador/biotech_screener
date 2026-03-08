"""
Company Pipeline & Trials Review Dashboard
Data: Company_Pipeline_Summary.csv (company), Biotech_Pipeline_Master.csv (asset), Enriched_Clinical_Trials.csv (trials)
Created by Tony Jiang
"""

import io
import json
import os
import pathlib
import re
from datetime import datetime
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st
import yfinance as yf
from dotenv import load_dotenv
from google import genai

try:
    from docx import Document as DocxDocument
except ImportError:
    DocxDocument = None  # python-docx not installed

load_dotenv(pathlib.Path(__file__).parent / ".env")

# 优先读 Streamlit Secrets（Cloud 部署），回退到环境变量（本地 .env）
def _get_secret(key: str) -> str:
    try:
        return st.secrets.get(key, "") or os.environ.get(key, "")
    except Exception:
        return os.environ.get(key, "")

_GEMINI_API_KEY = _get_secret("GEMINI_API_KEY")
_gemini_client = genai.Client(api_key=_GEMINI_API_KEY) if _GEMINI_API_KEY else None
GEMINI_FLASH_MODEL = "gemini-2.5-flash"

# ── Page config & custom CSS ───────────────────────────────────

st.set_page_config(
    page_title="Biotech Intelligence Dashboard | Tony Jiang",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    /* Dashboard title in header bar - white text on blue */
    header[data-testid="stHeader"] {
        background: linear-gradient(90deg, #1e3a5f 0%, #2d5a87 100%);
    }
    header[data-testid="stHeader"]::before {
        content: "Biotech Intelligence Dashboard for Non-oncology Companies";
        color: #ffffff;
        font-size: 1.35rem;
        font-weight: 600;
        display: block;
        padding: 0.5rem 0;
        white-space: nowrap;
    }
    /* Maximize main content area */
    .block-container { padding-top: 0.75rem; padding-bottom: 0.5rem; padding-left: 1.5rem; padding-right: 1.5rem; max-width: 100%; }
    .stDataFrame { font-size: 0.9rem; }
    /* Compact metrics */
    [data-testid="stMetricValue"] { font-size: 1.1rem; }
    /* Footer */
    .footer { text-align: right; font-size: 0.8rem; color: #6b7280; margin-top: 1rem; padding-top: 0.5rem; border-top: 1px solid #e5e7eb; }
    /* Section headers */
    .section-title { font-size: 1.05rem; font-weight: 600; color: #1e3a5f; margin-bottom: 0.25rem; }
    .company-detail-header { background: #f0f4f8; padding: 0.6rem 1rem; border-radius: 6px; margin-bottom: 0.5rem; border-left: 4px solid #2d5a87; }
    /* Main title: larger, single line */
    h1 { font-size: 1.75rem !important; white-space: nowrap; }
    /* 禁用侧边栏收起：隐藏折叠按钮 */
    [data-testid="collapsedControl"] { display: none !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

DATA_DIR = pathlib.Path(__file__).parent
SUMMARY_CSV = DATA_DIR / "Company_Pipeline_Summary.csv"
MASTER_CSV = DATA_DIR / "Biotech_Pipeline_Master.csv"
TRIALS_CSV = DATA_DIR / "Enriched_Clinical_Trials.csv"
ASSOCIATED_FILES_DIR = DATA_DIR / "associated_files"
ASSOCIATED_FILES_INDEX_CSV = DATA_DIR / "associated_files_index.csv"
AI_DD_REPORT_DIR = DATA_DIR / "AI_DD_REPORT"


def _ensure_company_files_dir(symbol: str) -> pathlib.Path:
    """Ensure associated_files / {Symbol} exists; return that path."""
    p = ASSOCIATED_FILES_DIR / symbol.strip().upper()
    p.mkdir(parents=True, exist_ok=True)
    return p


def _load_associated_index() -> pd.DataFrame:
    if not ASSOCIATED_FILES_INDEX_CSV.exists():
        df = pd.DataFrame(columns=["Symbol", "FilePath", "DisplayName", "UploadedAt"])
    else:
        df = pd.read_csv(ASSOCIATED_FILES_INDEX_CSV, encoding="utf-8-sig")
    # 每次加载时根据磁盘目录扫描，把已有文件并入 index（刷新后仍能认出）
    df, changed = _sync_associated_index_from_disk(df)
    if changed:
        _save_associated_index(df)
    return df


def _sync_associated_index_from_disk(existing: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    """扫描 associated_files/{Symbol}/ 下所有文件，将不在 index 中的加入。返回 (合并后的 df, 是否有新增)。"""
    if not ASSOCIATED_FILES_DIR.exists():
        return existing, False
    existing_paths: set[tuple[str, str]] = set()
    if not existing.empty and "Symbol" in existing.columns and "FilePath" in existing.columns:
        existing_paths = {
            (str(sym).strip().upper(), str(fp).strip().replace("\\", "/"))
            for sym, fp in zip(existing["Symbol"], existing["FilePath"])
        }
    new_rows = []
    for symbol_dir in ASSOCIATED_FILES_DIR.iterdir():
        if not symbol_dir.is_dir():
            continue
        sym = symbol_dir.name.upper()
        for f in symbol_dir.iterdir():
            if not f.is_file():
                continue
            rel = f"{sym}/{f.name}".replace("\\", "/")
            if (sym, rel) in existing_paths:
                continue
            try:
                uploaded_at = datetime.fromtimestamp(f.stat().st_mtime).isoformat()
            except Exception:
                uploaded_at = datetime.now().isoformat()
            new_rows.append({
                "Symbol": sym,
                "FilePath": rel,
                "DisplayName": f.name,
                "UploadedAt": uploaded_at,
            })
            existing_paths.add((sym, rel))
    if not new_rows:
        return existing, False
    return pd.concat([existing, pd.DataFrame(new_rows)], ignore_index=True), True


def _save_associated_index(df: pd.DataFrame) -> None:
    df.to_csv(ASSOCIATED_FILES_INDEX_CSV, encoding="utf-8-sig", index=False)


def _safe_filename(name: str) -> str:
    return re.sub(r"[^\w\-_.]", "_", name)[:80]


def _ensure_ai_dd_report_dir(symbol: str) -> pathlib.Path:
    """Ensure AI_DD_REPORT/{Symbol} exists; return that path."""
    p = AI_DD_REPORT_DIR / symbol.strip().upper()
    p.mkdir(parents=True, exist_ok=True)
    return p


def _build_dd_prompt(
    selected_symbol: str,
    company_name: str,
    market_cap: float,
    total_cash: float,
    pipeline_rows: pd.DataFrame,
) -> str:
    """组装终极 DD Prompt 模板。"""
    def _fmt(val, default="N/A"):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return default
        return str(val).strip() or default
    market_cap_str = f"${market_cap / 1e9:.2f}B" if isinstance(market_cap, (int, float)) and market_cap and not pd.isna(market_cap) else "N/A"
    total_cash_str = f"${total_cash / 1e6:.0f}M" if isinstance(total_cash, (int, float)) and total_cash and not pd.isna(total_cash) else "N/A"
    lines = []
    for _, row in pipeline_rows.iterrows():
        asset = _fmt(row.get("Asset_Name"))
        phase = _fmt(row.get("Highest_Phase"))
        moa = _fmt(row.get("Mechanism_of_Action"))
        catalyst = _fmt(row.get("Next_Catalyst_Date"))
        lines.append(f"- 药物/代号: {asset} | 阶段: {phase} | 机制: {moa} | 催化剂: {catalyst}")
    pipeline_str = "\n".join(lines) if lines else "(无管线数据)"
    return f"""你现在是一位横跨华尔街顶级投行（熟悉估值模型与并购逻辑）与顶尖药企临床研发（精通试验设计与统计学底牌）的资深 Biotech 量化投研专家。

你的任务是基于我提供的公司宏观基本面、临床管线数据、ATTACH 的材料，以及你自己通过 Deep Research 搜索到的全网最新情报（务必深度挖掘 SEC 10-K/10-Q 财报、财报电话会 Earnings Call 逐字稿、医学会议如 JPM/ASCO/ESMO/ASH 的最新 Data Readout），为我撰写一份极其硬核、冷酷、客观的投资尽职调查报告（DD Report）。

【输入数据上下文】
公司名称与代码：{selected_symbol} — {company_name}
当前市值：{market_cap_str}
账上现金：{total_cash_str}
核心管线列表：
{pipeline_str}

请严格按照以下 7 个模块输出研报，拒绝废话，直击投资痛点：

### 1. 管理层基本盘 (Management & Track Record)
- **团队背景**：核心高管与研发负责人的过往履历。过去是否有成功将药物推进至获批（FDA Approval）或成功将公司卖给 Big Pharma 的 Track Record？

### 2. 科学机制与临床/统计学推演 (Science & Clinical Data)
针对进度最快或价值最高的 1-2 个核心管线进行深度拆解：
- **MoA 护城河**：其作用机制（MoA）是 First-in-class，还是已有验证的 Validated target，抑或是完全 Novel 的盲盒？
- **试验设计审视**：拆解其核心临床试验。对照组（Comparator/SOC）选择是否合理？终点设计（Endpoints）是否存在水分？
- **数据透视**：挖掘其 Pre-clinical 与已公布的 Clinical 数据并进行严格对比。

### 3. 竞争格局与"中国 Biotech"防线 (Competitive Space & Moat)
- **赛道全景**：在同一适应症下，对比 In-class（同靶点）和 Out-of-class（治同种病）的竞品进度与数据。
- **反内卷护城河**：针对中国 Biotech 极速研发的 "Me-too/Fast-follower" 威胁，该公司的 Asset 是否具备足够深的护城河？

### 4. 商业化前景与市场空间 (Commercial Outlook & Unmet Need)
- **TAM 与痛点**：目标适应症的市场规模与真实的 Unmet Need。
- **商业化破局**：目前的 SOC 卖得如何？该公司准备如何实现差异化（Differentiate）？

### 5. 并购博弈与 Big Pharma 胃口 (M&A / BD Appetite)
- **历史先例**：梳理近期是否有 Big Pharma 收购类似靶点或适应症药物的先例与溢价。
- **潜在买家预测**：结合 Big Pharma 目前的管线短板，预测谁是最有可能的收购者？预估 Buyout Price 是多少？

### 6. 财务生存期与华尔街共识 (Financials, Valuation & Sentiment)
- **股价复盘与情绪**：分析过去一年股价异动原因，挖掘市场 Sentiment。
- **资金生死线**：基于最新 Burn Rate 和账上现金计算其实际 Runway。这笔钱能否撑到下一个数据读出之后？是否存在极高的数据公布前定增砸盘风险？
- **估值与目标价**：汇总华尔街 Target Price。结合 rNPV 或 DCF，给出一个大模型独立预测的 Target Price 并给出推导理由。

### 7. 最终审判局与情景假设 (The Final Verdict & Scenarios)
- **生死局总结**：判定估值是被严重低估还是高估？
- **情景推演**：牛市假设 (Bull Case) 能涨到多少？熊市假设 (Bear Case) 底线在哪？"""


def _list_report_files(symbol: str) -> list[tuple[str, str]]:
    """返回 [(文件名, 人类可读时间), ...]，按时间倒序。"""
    dir_path = AI_DD_REPORT_DIR / symbol.strip().upper()
    if not dir_path.exists() or not dir_path.is_dir():
        return []
    result = []
    for f in dir_path.iterdir():
        if f.is_file() and f.name.startswith("Report_") and f.suffix.lower() in (".md", ".txt", ".gdoc", ".doc", ".docx"):
            stem = f.stem  # Report_20260301_143000
            if stem.startswith("Report_"):
                ts_str = stem[7:]  # 20260301_143000
                try:
                    dt = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
                    human = dt.strftime("%Y-%m-%d %H:%M:%S")
                    result.append((f.name, human))
                except ValueError:
                    result.append((f.name, f.name))
    result.sort(key=lambda x: x[0], reverse=True)
    return result


def _cell_to_md(s: str) -> str:
    """单元格内容转 Markdown 安全：换行改空格，管道符转全角避免破坏表格。"""
    if not s:
        return ""
    t = s.strip().replace("\n", " ").replace("\r", " ")
    return t.replace("|", "｜")  # 全角管道符，避免破坏 Markdown 表格


def _table_to_markdown(table) -> str:
    """将 python-docx Table 转为 Markdown 表格字符串。"""
    rows = []
    for row in table.rows:
        cells = [_cell_to_md(cell.text) for cell in row.cells]
        if any(cells):
            rows.append("| " + " | ".join(cells) + " |")
    if not rows:
        return ""
    # 表头分隔行
    n_cols = len(table.rows[0].cells)
    sep = "| " + " | ".join(["---"] * n_cols) + " |"
    return rows[0] + "\n" + sep + "\n" + "\n".join(rows[1:])


def _extract_docx_text(file_path: pathlib.Path) -> tuple[str, str | None]:
    """从 .docx 文件提取正文与表格，表格以 Markdown 形式保留。返回 (正文, 错误信息)。"""
    if DocxDocument is None:
        return "", "未安装 python-docx。请在终端运行: pip install python-docx"
    try:
        doc = DocxDocument(io.BytesIO(file_path.read_bytes()))
        parts = []
        # 按文档顺序：段落与表格交错（python-docx 的 paragraphs/tables 是分开的，需用 element.body 保序）
        from docx.text.paragraph import Paragraph
        from docx.table import Table
        for el in doc.element.body:
            tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
            if tag == "p":
                p = Paragraph(el, doc)
                if p.text and p.text.strip():
                    parts.append(p.text.strip())
            elif tag == "tbl":
                t = Table(el, doc)
                md = _table_to_markdown(t)
                if md:
                    parts.append(md)
        # 若上面保序失败（兼容旧版），则回退为段落+表格
        if not parts:
            for p in doc.paragraphs:
                if p.text and p.text.strip():
                    parts.append(p.text.strip())
            for table in doc.tables:
                md = _table_to_markdown(table)
                if md:
                    parts.append(md)
        # 页眉、页脚
        for section in doc.sections:
            for para in section.header.paragraphs:
                if para.text and para.text.strip():
                    parts.append(para.text.strip())
            for para in section.footer.paragraphs:
                if para.text and para.text.strip():
                    parts.append(para.text.strip())
        text = "\n\n".join(parts).strip()
        return text, None
    except Exception as e:
        return "", str(e)


def _gemini_summarize_report(report_text: str) -> tuple[str | None, str | None]:
    """调用 Gemini Flash 对研报做总结。返回 (总结文本, 错误信息)，成功时错误为 None。"""
    if not _gemini_client:
        return None, "未配置 GEMINI_API_KEY，请在 .env 中设置。"
    if not (report_text or "").strip():
        return None, "当前研报无正文可总结（如为 .doc 或 .gdoc 请先下载查看后复制内容）。"
    prompt = """你是一位 Biotech 投研专家。请对以下尽职调查报告（DD Report）做简明总结。

要求：
- 用中文输出
- 保留核心结论、主要风险与投资要点
- 分点或分段落，条理清晰
- 控制在 500 字以内

报告原文：
---
"""
    prompt += (report_text or "").strip()
    try:
        resp = _gemini_client.models.generate_content(
            model=GEMINI_FLASH_MODEL,
            contents=prompt,
        )
        text = (resp.text or "").strip()
        return (text, None) if text else (None, "API 返回为空")
    except Exception as e:
        return None, str(e)


# Same control-token filter as build_pipeline (for display-only cleaning)
_CTRL_PATTERN = re.compile(
    r"\b(placebo|sham|standard\s+of\s+care|soc|best\s+supportive\s+care|bsc|"
    r"saline|normal\s+saline|inactive|comparator|control|dummy)\b",
    re.IGNORECASE,
)
# 明确跳过 “MATCHING PLACEBO / PLACEBO MATCHING” 等变体（不限长度）
_PLACEBO_VARIANT_PATTERN = re.compile(
    r"\bmatching\s+placebo\b|\bplacebo\s+matching\b|\bplacebo\s+for\s+|\bplacebo\s+of\s+|\bplacebo\s+to\s+match|"
    r"\bplacebo\s+control\b|\bplacebo\s+comparator\b|\bthe\s+placebo\b|"
    r"\bmatched\s+placebo\b|\bplacebo\s+matched\b|\bplacebo\s+in\s+",
    re.IGNORECASE,
)
# 用于展示时过滤 Master 中的 placebo 类 asset（与 build_pipeline 的 is_placebo_asset 逻辑一致）
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
# Strip trailing dose specs (e.g. "0.5 mg", "10 mg/kg QD") so "ALKS 2680 0.5mg" -> "ALKS 2680"
_DOSE_SUFFIX = re.compile(
    r"\s+\d+(?:\.\d+)?\s*(?:mg|mcg|µg|g|kg|iu|ui|ml|%|mg/kg|mcg/kg|µg/kg|mg/day|mcg/day)(?:\s*(?:qd|bid|tid|daily|weekly|twice|once))?\s*$",
    re.IGNORECASE,
)
# Token that is only a dose (e.g. "4mg", "6 mg", "10mg QD") — skip these, we only want drug names
_DOSE_ONLY_PATTERN = re.compile(
    r"^\s*\d+(?:\.\d+)?\s*(?:mg|mcg|µg|g|kg|iu|ui|ml|%|mg/kg|mcg/kg|µg/kg|mg/day|mcg/day)(?:\s*(?:qd|bid|tid|daily|weekly|twice|once))?\s*$",
    re.IGNORECASE,
)
# 清洗 TREATMENT 类型前缀/后缀：OPEN LABEL, BLINDED, Open-Label, Double-Blind, High/Low Dose 等
_TREATMENT_STRIP_LEADING = re.compile(
    r"^\s*(?:open\s*[-]?\s*label\s+(?:extension\s*[-]?\s*period\s*)?|blinded\s+|double\s*[-]?\s*blind(?:ed)?\s+|single\s*[-]?\s*blind(?:ed)?\s+|"
    r"high\s+dose\s+of\s+|low\s+dose\s+of\s+|high\s+dose\s+|low\s+dose\s+)+",
    re.IGNORECASE,
)
_TREATMENT_STRIP_TRAILING = re.compile(
    r"\s+(?:open\s*[-]?\s*label|blinded|double\s*[-]?\s*blind|single\s*[-]?\s*blind|high\s+dose|low\s+dose)\s*$",
    re.IGNORECASE,
)


def _is_placebo_asset(name: str) -> bool:
    """与 build_pipeline 一致：若为 placebo/control 类 asset 则返回 True，展示时过滤。"""
    if not name or not str(name).strip():
        return False
    return bool(_PLACEBO_ASSET_PATTERN.match(str(name).strip()))


def _strip_treatment_prefix_suffix(s: str) -> str:
    """Strip OPEN LABEL / BLINDED / Open-Label / High Dose / Low Dose 等前缀与后缀，便于归一化显示。"""
    if not s or not str(s).strip():
        return s
    t = str(s).strip()
    while _TREATMENT_STRIP_LEADING.search(t):
        t = _TREATMENT_STRIP_LEADING.sub("", t, count=1).strip()
    while _TREATMENT_STRIP_TRAILING.search(t):
        t = _TREATMENT_STRIP_TRAILING.sub("", t, count=1).strip()
    return t.strip() or s


def _normalize_asset_for_grouping(asset_name: str) -> str:
    """
    与 build_pipeline 一致：同一药物的多种写法归一为同一 key（Blinded ESK-001 / Open-Label ESK-001 -> ESK-001），
    用于展示时按药物合并行。
    """
    if not asset_name or not str(asset_name).strip():
        return str(asset_name).strip() or ""
    t = _strip_treatment_prefix_suffix(str(asset_name).strip())
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"([A-Za-z0-9])\s+(\d)", r"\1-\2", t)
    return t.strip() or str(asset_name).strip()


def _normalize_intervention_base(s: str) -> str:
    """Remove trailing dose suffix and treatment prefix/suffix so same drug maps to one name."""
    t = s.strip()
    while _DOSE_SUFFIX.search(t):
        t = _DOSE_SUFFIX.sub("", t).strip()
    t = _strip_treatment_prefix_suffix(t)
    return t


def _interventions_display(raw: str) -> str:
    """Strip placebo/control and dose-only tokens; show only drug names, deduplicated (no doses)."""
    if not raw or str(raw).strip() in ("", "N/A"):
        return ""
    parts = re.split(r"\s*[,|]\s*", str(raw))
    kept_bases = []
    seen_bases = set()
    for p in parts:
        p = p.strip()
        if not p:
            continue
        # 明确跳过 “MATCHING PLACEBO / PLACEBO MATCHING” 等变体（不限长度）
        if _PLACEBO_VARIANT_PATTERN.search(p):
            continue
        if _CTRL_PATTERN.search(p) and len(p) < 50:
            continue
        # Skip tokens that are purely a dose (e.g. "4mg", "6 mg", "10mg QD")
        if _DOSE_ONLY_PATTERN.match(p):
            continue
        base = _normalize_intervention_base(p)
        if not base:
            continue
        key = base.upper()
        if key in seen_bases:
            continue
        seen_bases.add(key)
        kept_bases.append(base)
    return ", ".join(kept_bases) if kept_bases else str(raw).strip()


@st.cache_data(show_spinner="Loading company summary…")
def load_summary() -> pd.DataFrame:
    if not SUMMARY_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(SUMMARY_CSV)
    for col in ["Market Cap", "EV", "Total Debt", "Total Cash", "Price", "52W Low", "52W High", "Wall Street Ratings",
                "Pipeline_Count", "Total_Active_Trials", "Shares Outstanding", "Institutional Shares", "Insider %"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


@st.cache_data(show_spinner="Loading trials…")
def load_trials() -> pd.DataFrame:
    if not TRIALS_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(TRIALS_CSV)
    for col in ["EnrollmentCount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


@st.cache_data(show_spinner="Loading pipeline master…")
def load_master() -> pd.DataFrame:
    """中间层：公司 -> 药物管线 (Biotech_Pipeline_Master.csv)，不修改内容与结构，仅读取。"""
    if not MASTER_CSV.exists():
        return pd.DataFrame()
    return pd.read_csv(MASTER_CSV)


def get_realtime_financials(symbol: str) -> dict:
    """
    从 yfinance 获取单只股票的实时财务与华尔街共识数据。
    使用 .get() 安全提取，缺失填 'N/A'。返回 dict 含：
    Price, Market Cap, Total Cash, targetHighPrice, targetMeanPrice, targetLowPrice,
    recommendationKey（首字母大写美化）, numberOfAnalystOpinions。
    """
    if not symbol or str(symbol).strip() == "":
        return _empty_financials()
    sym = str(symbol).strip().upper()
    try:
        ticker = yf.Ticker(sym)
        info = ticker.info or {}
    except Exception:
        return _empty_financials()
    def _num(val, default="N/A"):
        if val is None:
            return default
        try:
            return float(val)
        except (TypeError, ValueError):
            return default
    # 评级：recommendationKey 如 "buy", "strong_buy" -> "Buy", "Strong Buy"
    rec_raw = info.get("recommendationKey") or info.get("recommendation")
    if rec_raw is not None and str(rec_raw).strip():
        rec = str(rec_raw).replace("_", " ").strip().title()
    else:
        rec = "N/A"
    return {
        "Price": _num(info.get("currentPrice") or info.get("regularMarketPrice")),
        "Market Cap": _num(info.get("marketCap")),
        "Total Cash": _num(info.get("totalCash")),
        "targetHighPrice": _num(info.get("targetHighPrice")),
        "targetMeanPrice": _num(info.get("targetMeanPrice")),
        "targetLowPrice": _num(info.get("targetLowPrice")),
        "recommendationKey": rec,
        "numberOfAnalystOpinions": _num(info.get("numberOfAnalystOpinions")),
    }


def _empty_financials() -> dict:
    return {
        "Price": "N/A",
        "Market Cap": "N/A",
        "Total Cash": "N/A",
        "targetHighPrice": "N/A",
        "targetMeanPrice": "N/A",
        "targetLowPrice": "N/A",
        "recommendationKey": "N/A",
        "numberOfAnalystOpinions": "N/A",
    }


@st.cache_data(show_spinner="Fetching analyst upside…", ttl=3600)
def get_analyst_upside_batch(symbols: tuple) -> dict:
    """
    批量获取各 symbol 的 Upside %（(targetMeanPrice - currentPrice) / currentPrice * 100）。
    返回 dict: symbol -> float 或 None（无覆盖时为 None）。symbols 为 tuple 以便可哈希缓存。
    """
    result = {}
    for sym in symbols:
        if not sym or str(sym).strip() == "":
            result[sym] = None
            continue
        sym = str(sym).strip().upper()
        try:
            ticker = yf.Ticker(sym)
            info = ticker.info or {}
            price = info.get("currentPrice") or info.get("regularMarketPrice")
            target = info.get("targetMeanPrice")
            if price is not None and target is not None:
                try:
                    p, t = float(price), float(target)
                    if p > 0:
                        result[sym] = round((t - p) / p * 100.0, 2)
                    else:
                        result[sym] = None
                except (TypeError, ValueError):
                    result[sym] = None
            else:
                result[sym] = None
        except Exception:
            result[sym] = None
    return result


# ── Load data ─────────────────────────────────────────────────────

df_summary = load_summary()
df_trials_all = load_trials()
df_master = load_master()

if df_summary.empty:
    st.error(f"**{SUMMARY_CSV.name}** not found. Run the pipeline first.")
    st.stop()

# ── Sidebar: filters ──────────────────────────────────────────────

st.sidebar.markdown("### Filters")
st.sidebar.markdown("---")

# Market Cap (B)
mcap_b = df_summary["Market Cap"].dropna() / 1e9
mcap_min_b = float(mcap_b.min()) if len(mcap_b) else 0.0
mcap_max_b = float(mcap_b.max()) if len(mcap_b) else 1.0
st.sidebar.markdown("**Market Cap (B USD)**")
mcap_lo = st.sidebar.number_input("Min", min_value=0.0, max_value=mcap_max_b * 1.2, value=mcap_min_b, step=0.1, format="%.2f", key="mcap_lo")
mcap_hi = st.sidebar.number_input("Max", min_value=0.0, max_value=mcap_max_b * 2, value=mcap_max_b, step=0.5, format="%.2f", key="mcap_hi")

# Therapeutic Area
tas = sorted(df_summary["Therapeutic_Area_Filter"].dropna().unique().tolist())
# 仅显示固定 TA 列表，且去掉 Oncology（公司保留，仅从筛选项隐藏）
TA_FILTER_ALLOWED = frozenset([
    "Cardiovascular", "Dermatology", "Gastroenterology", "Hematology",
    "Immunology", "Infectious Diseases", "Metabolic/Endocrinology", "Musculoskeletal",
    "Neurology/CNS", "No Trials", "Ophthalmology", "Others", "Pain", "Rare Disease",
    "Respiratory", "Urology/Nephrology",
])
tas = [t for t in tas if t in TA_FILTER_ALLOWED]
ta_selected = st.sidebar.multiselect("Therapeutic Area", options=tas, default=[], key="ta_filter")

# Phase
_phase_order = {"PHASE4": 4, "PHASE3": 3, "PHASE2": 2, "PHASE1": 1, "EARLY_PHASE1": 0, "N/A": -1, "No Trials": -2}
phases_raw = df_summary["Highest_Phase"].dropna().unique().tolist()
phases = sorted(phases_raw, key=lambda x: (_phase_order.get(x, -1), str(x)))
phase_selected = st.sidebar.multiselect("Phase of Development", options=phases, default=[], key="phase_filter")

# Has Marketed Drug
marketed_options = ["All", "Yes", "No"]
marketed_choice = st.sidebar.radio("Has Marketed Drug", options=marketed_options, index=0, key="marketed")

# Upside % (华尔街目标价潜在涨幅，允许负数)
st.sidebar.markdown("**Upside %** (Target vs Current)")
upside_lo = st.sidebar.number_input("Min %", value=-999.0, step=1.0, format="%.1f", key="upside_lo", help="e.g. 10 = 10%+ upside; -999 = no lower bound")
upside_hi = st.sidebar.number_input("Max %", value=999.0, step=1.0, format="%.1f", key="upside_hi", help="e.g. 50 = up to 50%; 999 = no upper bound")

st.sidebar.markdown("---")
execute_btn = st.sidebar.button("Execute Search", type="primary", width="stretch")
clear_btn = st.sidebar.button("Clear filters", width="stretch")
refresh_btn = st.sidebar.button("🔄 Refresh data (reload CSV)", width="stretch", help="清缓存并重新读取 CSV。修改过 CSV 或跑过 normalize_ta_csvs 后请点此以看到最新 TA/公司数据。")
st.sidebar.markdown("---")
st.sidebar.caption("Dashboard by **Tony Jiang**")

# Session state for applied filters (apply only on Execute Search)
if "search_executed" not in st.session_state:
    st.session_state["search_executed"] = False
if execute_btn:
    st.session_state["applied_mcap_lo"] = mcap_lo
    st.session_state["applied_mcap_hi"] = mcap_hi
    st.session_state["applied_ta"] = ta_selected
    st.session_state["applied_phase"] = phase_selected
    st.session_state["applied_marketed"] = marketed_choice
    st.session_state["applied_upside_lo"] = upside_lo
    st.session_state["applied_upside_hi"] = upside_hi
    st.session_state["search_executed"] = True
    st.rerun()
if clear_btn:
    st.session_state["search_executed"] = False
    # 不能直接给 widget 的 key 赋值，否则会报错；删除 key 后 rerun，widget 会用默认值
    for key in ("mcap_lo", "mcap_hi", "ta_filter", "phase_filter", "marketed", "upside_lo", "upside_hi", "company_select"):
        if key in st.session_state:
            del st.session_state[key]
    st.rerun()
if refresh_btn:
    st.cache_data.clear()
    st.rerun()

# Use applied filter values when Execute Search was clicked; otherwise default = show all companies
if st.session_state["search_executed"] and "applied_mcap_lo" in st.session_state:
    _mcap_lo = st.session_state["applied_mcap_lo"]
    _mcap_hi = st.session_state["applied_mcap_hi"]
    _ta = st.session_state["applied_ta"]
    _phase = st.session_state["applied_phase"]
    _marketed = st.session_state["applied_marketed"]
    _upside_lo = st.session_state.get("applied_upside_lo", -999.0)
    _upside_hi = st.session_state.get("applied_upside_hi", 999.0)
else:
    # Default: show all companies (full range, no TA/phase/marketed/upside filter)
    _mcap_lo = mcap_min_b
    _mcap_hi = mcap_max_b
    _ta = []
    _phase = []
    _marketed = "All"
    _upside_lo = -999.0
    _upside_hi = 999.0

# ── Apply filters to summary (default = all; after Execute Search = applied values) ─

filtered = df_summary.copy()
filtered = filtered[
    (filtered["Market Cap"].fillna(0) >= _mcap_lo * 1e9) &
    (filtered["Market Cap"].fillna(0) <= _mcap_hi * 1e9)
]
if _ta:
    filtered = filtered[filtered["Therapeutic_Area_Filter"].isin(_ta)]
if _phase:
    filtered = filtered[filtered["Highest_Phase"].isin(_phase)]
if _marketed == "Yes":
    filtered = filtered[filtered["Has_Marketed_Drug"] == "Yes"]
elif _marketed == "No":
    filtered = filtered[filtered["Has_Marketed_Drug"] == "No"]
filtered = filtered.drop_duplicates(subset="Symbol", keep="first").reset_index(drop=True)

# Upside filter: 仅当用户设置了 Upside 区间时才批量拉取并过滤，避免首屏卡住
if _upside_lo > -999 or _upside_hi < 999:
    symbols_tuple = tuple(filtered["Symbol"].dropna().astype(str).str.strip().str.upper().unique().tolist())
    if symbols_tuple:
        upside_map = get_analyst_upside_batch(symbols_tuple)
        filtered["Upside_Pct"] = filtered["Symbol"].astype(str).str.strip().str.upper().map(upside_map)
        # 只保留有覆盖且在区间内的行（无覆盖的 NaN 被排除）
        filtered = filtered[
            filtered["Upside_Pct"].notna() &
            (filtered["Upside_Pct"] >= _upside_lo) &
            (filtered["Upside_Pct"] <= _upside_hi)
        ].reset_index(drop=True)
    # else: 无 symbol 时 filtered 保持不动
else:
    # 未启用 Upside 过滤时不拉取（避免首屏对全量 symbol 请求 yfinance 导致卡顿/无显示）
    # 表格不显示 Upside % 列；用户设置 Upside 区间并 Execute 后再显示
    pass

# Unique companies for selector (from filtered rows)
companies = filtered[["Symbol", "Name"]].drop_duplicates().sort_values("Name")
company_list = ["— Select company —"] + companies.apply(lambda r: f"{r['Symbol']} — {r['Name']}", axis=1).tolist()

# ── Header: caption under banner title ─────────────────────────────

st.caption("Use sidebar filters and **Execute Search** to apply · Select a company to filter list and view trials")
st.markdown("---")

# ── Top bar: KPI + Select Company ──────────────────────────────────

col_k1, col_k2, col_k3, col_sel = st.columns([1, 1, 1, 2])
with col_k1:
    st.metric("Companies", f"{len(filtered)}")
with col_k2:
    avg_mcap = filtered["Market Cap"].mean() / 1e9
    st.metric("Avg Market Cap", f"${avg_mcap:.2f}B" if pd.notna(avg_mcap) else "—")
with col_k3:
    with_catalyst = (filtered["Next_Catalyst"].notna()) & (filtered["Next_Catalyst"] != "") & (filtered["Next_Catalyst"] != "Passed")
    st.metric("With future catalyst", f"{with_catalyst.sum()}")
with col_sel:
    company_choice = st.selectbox(
        "Select company",
        options=company_list,
        index=0,
        key="company_select",
    )

# ── Display table: one row per company (or single company if selected) ────────
display = filtered.copy()
# When a company is selected, filter table to that company only
if company_choice and company_choice != "— Select company —":
    sel_symbol = company_choice.split(" — ")[0]
    display = display[display["Symbol"] == sel_symbol].copy()

# Format for display (keep originals for sorting)
display["Market_Cap_B"] = (display["Market Cap"] / 1e9).round(2)
display["EV_B"] = (display["EV"] / 1e9).round(2)
display["Total_Debt_M"] = (display["Total Debt"] / 1e6).round(0)
display["Total_Cash_M"] = (display["Total Cash"] / 1e6).round(0)

# Column order
cols_show = [
    "Symbol", "Price", "Market_Cap_B", "Name", "Therapeutic_Areas", "Has_Marketed_Drug",
    "Highest_Phase", "Pipeline_Count", "Total_Active_Trials", "Next_Catalyst",
    "Country", "52W Low", "52W High", "EV_B",
    "Shares Outstanding", "Institutional Shares", "Total_Debt_M", "Total_Cash_M",
    "Wall Street Ratings", "Upside_Pct",
]
cols_show = [c for c in cols_show if c in display.columns]
display = display[cols_show]

# Rename for display labels
display = display.rename(columns={
    "Market_Cap_B": "Market Cap (B)",
    "EV_B": "EV (B)",
    "Total_Debt_M": "Total Debt (M)",
    "Total_Cash_M": "Total Cash (M)",
    "Upside_Pct": "Upside %",
})

# Table height: small when 1 company selected so Trial info comes up
table_height = 100 if len(display) <= 1 else 520

st.markdown(
    "<p class='section-title'>Company list (one row per company)"
    + (" — filtered to selected company" if company_choice and company_choice != "— Select company —" else "")
    + " — sort by clicking column headers</p>",
    unsafe_allow_html=True,
)
st.dataframe(
    display.reset_index(drop=True),
    width="stretch",
    hide_index=True,
    height=table_height,
    column_config={
        "Price": st.column_config.NumberColumn("Price", format="$%.2f"),
        "Market Cap (B)": st.column_config.NumberColumn("Market Cap ($B)", format="%.2f"),
        "EV (B)": st.column_config.NumberColumn("EV ($B)", format="%.2f"),
        "52W Low": st.column_config.NumberColumn("52W Low", format="$%.2f"),
        "52W High": st.column_config.NumberColumn("52W High", format="$%.2f"),
        "Total Debt (M)": st.column_config.NumberColumn("Total Debt ($M)", format="%.0f"),
        "Total Cash (M)": st.column_config.NumberColumn("Total Cash ($M)", format="%.0f"),
        "Wall Street Ratings": st.column_config.NumberColumn("WS Rating", format="%.2f"),
        "Upside %": st.column_config.NumberColumn("Upside %", format="%.1f"),
        "Therapeutic_Areas": st.column_config.TextColumn("Therapeutic Areas", width="large"),
    },
)

# ── Company detail: 三级标签页（管线概览 | 研报生成器 | 研报阅读）───────────────────

if company_choice and company_choice != "— Select company —":
    selected_symbol = company_choice.split(" — ")[0]
    company_name = company_choice.split(" — ", 1)[1] if " — " in company_choice else ""

    st.markdown("---")
    st.markdown(
        f"<div class='company-detail-header'><strong>{selected_symbol}</strong> — {company_name}</div>",
        unsafe_allow_html=True,
    )

    tab1, tab2, tab3 = st.tabs(["📊 管线与临床概览", "🧠 智能研报生成器", "📂 研报阅读与投研笔记"])

    # ── Tab 1: 管线与临床概览 ─────────────────────────────────────────────
    with tab1:
        _company_row = None
        if selected_symbol and not df_summary[df_summary["Symbol"] == selected_symbol].empty:
            _company_row = df_summary[df_summary["Symbol"] == selected_symbol].iloc[0]
        _website = str(_company_row.get("Website", "")).strip() if _company_row is not None and "Website" in df_summary.columns else ""
        _ir_link = str(_company_row.get("IR_Search_Link", "")).strip() if _company_row is not None and "IR_Search_Link" in df_summary.columns else ""
        if _website in ("", "nan", "N/A"):
            _website = f"https://www.google.com/search?q={quote_plus(company_name)}"
        if _ir_link in ("", "nan", "N/A"):
            _ir_link = f"https://www.google.com/search?q={quote_plus(company_name + ' investor presentation PDF')}"
        _link_cols = st.columns([1, 1, 4])
        with _link_cols[0]:
            st.link_button("🌐 Official Website", _website)
        with _link_cols[1]:
            st.link_button("📄 IR / Investor Decks", _ir_link)

        with st.spinner("Loading analyst data…"):
            _fin = get_realtime_financials(selected_symbol)
        with st.expander("🏦 华尔街共识 (Wall Street Consensus)", expanded=True):
            _price = _fin.get("Price")
            _target_mean = _fin.get("targetMeanPrice")
            _has_target = isinstance(_target_mean, (int, float)) and _target_mean not in (None, "")
            _has_price = isinstance(_price, (int, float)) and _price not in (None, "") and (_price or 0) > 0
            if not _has_target or not _has_price:
                st.info("No Analyst Coverage — 该公司暂无华尔街目标价/共识数据。")
            else:
                _upside_pct = (_target_mean - _price) / _price * 100.0
                c1, c2, c3, c4, c5, c6 = st.columns(6)
                with c1:
                    st.metric("Current Price", f"${_price:.2f}" if isinstance(_price, (int, float)) else str(_price))
                with c2:
                    _th = _fin.get("targetHighPrice")
                    st.metric("Target High", f"${_th:.2f}" if isinstance(_th, (int, float)) else str(_th))
                with c3:
                    st.metric("Target Mean", f"${_target_mean:.2f}")
                with c4:
                    _tl = _fin.get("targetLowPrice")
                    st.metric("Target Low", f"${_tl:.2f}" if isinstance(_tl, (int, float)) else str(_tl))
                with c5:
                    st.metric("Consensus", _fin.get("recommendationKey", "N/A"))
                with c6:
                    _n = _fin.get("numberOfAnalystOpinions")
                    st.metric("Analysts", str(int(_n)) if isinstance(_n, (int, float)) else str(_n))
                _upside_str = f"{_upside_pct:+.1f}%"
                if _upside_pct > 0:
                    st.markdown(f"<span style='color: #16a34a; font-weight: 600;'>Upside: {_upside_str}</span>", unsafe_allow_html=True)
                else:
                    st.markdown(f"<span style='color: #b91c1c; font-weight: 600;'>Upside: {_upside_str}</span>", unsafe_allow_html=True)

        st.markdown("**Associated Files**")
        _company_dir = _ensure_company_files_dir(selected_symbol)
        _af_index = _load_associated_index()
        _files_for_symbol = _af_index[_af_index["Symbol"].astype(str).str.strip().str.upper() == selected_symbol.upper()] if not _af_index.empty and selected_symbol else pd.DataFrame()
        _up_col1, _up_col2 = st.columns([2, 1])
        with _up_col1:
            _uploaded = st.file_uploader("Upload any file to associate with this company", type=None, key=f"assoc_file_{selected_symbol}")
        with _up_col2:
            if _uploaded is not None and selected_symbol:
                if st.button("Upload & associate with " + selected_symbol, type="primary", key=f"assoc_btn_{selected_symbol}"):
                    _ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    _stored_name = f"{_ts}_{_safe_filename(_uploaded.name)}"
                    _rel_path = f"{selected_symbol.upper()}/{_stored_name}"
                    _path = ASSOCIATED_FILES_DIR / selected_symbol.upper() / _stored_name
                    _path.write_bytes(_uploaded.getvalue())
                    _new = pd.DataFrame([{"Symbol": selected_symbol.upper(), "FilePath": _rel_path, "DisplayName": _uploaded.name, "UploadedAt": datetime.now().isoformat()}])
                    _af_index = _load_associated_index()
                    _save_associated_index(pd.concat([_af_index, _new], ignore_index=True))
                    st.success("Saved and associated with " + selected_symbol)
                    st.rerun()
        if not _files_for_symbol.empty:
            st.caption("Associated file(s) — download or delete:")
            for _idx, _row in _files_for_symbol.iterrows():
                _rel = _row["FilePath"]
                _fpath = ASSOCIATED_FILES_DIR / _rel
                _dname = _row.get("DisplayName", _rel.split("/")[-1])
                _c1, _c2 = st.columns([3, 1])
                with _c1:
                    if _fpath.exists():
                        _bytes = _fpath.read_bytes()
                        st.download_button(label=f"📎 {_dname}", data=_bytes, mime="application/octet-stream", file_name=_dname, key=f"dl_af_{selected_symbol}_{_idx}")
                    else:
                        st.caption(f"Missing: {_rel}")
                with _c2:
                    if st.button("Delete", key=f"del_af_{selected_symbol}_{_idx}"):
                        if _fpath.exists():
                            _fpath.unlink()
                        _af_index = _load_associated_index()
                        _save_associated_index(_af_index[_af_index["FilePath"] != _rel].reset_index(drop=True))
                        st.rerun()

        assets = pd.DataFrame()
        if not df_master.empty and "Symbol" in df_master.columns:
            assets = df_master.loc[df_master["Symbol"].astype(str).str.strip().str.upper() == selected_symbol.upper()].copy()
        _phase_order = {"PHASE4": 4, "PHASE3": 3, "PHASE2": 2, "PHASE1": 1, "EARLY_PHASE1": 0, "N/A": -1, "No Trials": -2}
        if not assets.empty and "Highest_Phase" in assets.columns:
            assets["_phase_ord"] = assets["Highest_Phase"].map(lambda x: _phase_order.get(x, -1))
            assets = assets.sort_values("_phase_ord", ascending=False).drop(columns=["_phase_ord"], errors="ignore")
        if not assets.empty and "Asset_Name" in assets.columns:
            assets = assets[~assets["Asset_Name"].apply(lambda x: _is_placebo_asset(x) if pd.notna(x) else False)]
        if not assets.empty:
            assets["_norm"] = assets["Asset_Name"].apply(lambda x: _normalize_asset_for_grouping(str(x)) if pd.notna(x) else "")
            assets = assets[assets["_norm"].astype(str).str.len() > 0].copy()
        if assets.empty:
            st.markdown("<p class='section-title'>药物管线 · Drugs</p>", unsafe_allow_html=True)
            st.info("未在 Biotech_Pipeline_Master.csv 中找到该公司的药物管线记录。")
        else:
            _phase_rank = {"PHASE4": 5, "PHASE3": 4, "PHASE2": 3, "PHASE1": 2, "EARLY_PHASE1": 1, "N/A": 0, "No Trials": -1}
            def _merge_assets(grp):
                row = grp.iloc[0].to_dict()
                row["Asset_Name"] = grp["_norm"].iloc[0]
                if grp["Highest_Phase"].notna().any():
                    best = grp["Highest_Phase"].map(lambda p: _phase_rank.get(p, -1)).idxmax()
                    row["Highest_Phase"] = grp.loc[best, "Highest_Phase"]
                row["Active_Trial_Count"] = grp["Active_Trial_Count"].apply(lambda x: int(x) if pd.notna(x) else 0).sum()
                nct_list = []
                for s in grp["Trial_NCTIds"].dropna():
                    for t in re.split(r"[,;]\s*", str(s)):
                        t = t.strip()
                        if t and (t.upper().startswith("NCT") or t.isdigit()):
                            nct_list.append(t if t.upper().startswith("NCT") else f"NCT{t}")
                row["Trial_NCTIds"] = ", ".join(dict.fromkeys(nct_list))
                cond_set = set()
                for c in grp["Detailed_Conditions"].dropna():
                    for item in str(c).split(";"):
                        if item.strip():
                            cond_set.add(item.strip())
                row["Detailed_Conditions"] = "; ".join(sorted(cond_set)) if cond_set else (grp["Detailed_Conditions"].iloc[0] if grp["Detailed_Conditions"].notna().any() else "")
                dates = grp["Next_Catalyst_Date"].dropna().astype(str).str.strip()
                dates = dates[~dates.isin(("", "N/A", "Passed"))]
                row["Next_Catalyst_Date"] = dates.iloc[0] if len(dates) else (grp["Next_Catalyst_Date"].iloc[0] if grp["Next_Catalyst_Date"].notna().any() else "")
                for col in ["Mechanism_of_Action", "Therapeutic_Area", "Market_Status"]:
                    if col in grp.columns:
                        v = grp[col].dropna()
                        row[col] = v.iloc[0] if len(v) else grp[col].iloc[0]
                return pd.Series(row)
            assets = assets.groupby("_norm", as_index=False).apply(_merge_assets).reset_index(drop=True)
            assets = assets.drop(columns=["_norm"], errors="ignore")
            assets_display = assets.copy()
            drug_cols = ["Asset_Name", "Highest_Phase", "Market_Status", "Active_Trial_Count", "Next_Catalyst_Date", "Mechanism_of_Action", "Therapeutic_Area", "Detailed_Conditions", "Trial_NCTIds"]
            drug_cols = [c for c in drug_cols if c in assets_display.columns]
            assets_display = assets_display[drug_cols]
            st.markdown("<p class='section-title'>药物管线 · Drugs</p>", unsafe_allow_html=True)
            st.dataframe(assets_display.reset_index(drop=True), width="stretch", hide_index=True, height=min(400, 120 + len(assets_display) * 36), column_config={
                "Asset_Name": st.column_config.TextColumn("Asset / 药物", width="medium"),
                "Active_Trial_Count": st.column_config.NumberColumn("Trials", format="%d"),
                "Next_Catalyst_Date": st.column_config.TextColumn("Catalyst", width="small"),
                "Mechanism_of_Action": st.column_config.TextColumn("MoA", width="medium"),
                "Therapeutic_Area": st.column_config.TextColumn("TA", width="medium"),
                "Detailed_Conditions": st.column_config.TextColumn("Conditions", width="large"),
                "Trial_NCTIds": st.column_config.TextColumn("NCT IDs", width="large"),
            })
        st.markdown("<p class='section-title'>试验 · Trials</p>", unsafe_allow_html=True)
        trials = df_trials_all[df_trials_all["Symbol"].astype(str).str.strip().str.upper() == selected_symbol.upper()].copy() if not df_trials_all.empty else pd.DataFrame()
        if trials.empty:
            st.info("该公司在 Enriched_Clinical_Trials.csv 中暂无试验记录。")
        else:
            trial_cols = ["NCTId", "Phases", "Status", "Conditions", "Interventions", "EnrollmentCount", "StartDate", "PrimaryCompletionDate", "BriefSummary", "OfficialTitle"]
            trial_cols = [c for c in trial_cols if c in trials.columns]
            trials_display = trials[trial_cols].copy()
            if "Interventions" in trials_display.columns:
                trials_display["Interventions"] = trials_display["Interventions"].apply(_interventions_display)
            trials_display["NCT_Link"] = trials_display["NCTId"].apply(lambda x: f"https://clinicaltrials.gov/study/{x}" if pd.notna(x) and str(x).strip().upper().startswith("NCT") else "")
            st.dataframe(trials_display, width="stretch", hide_index=True, height=min(450, 120 + len(trials_display) * 38), column_config={
                "NCTId": st.column_config.TextColumn("NCT ID", width="small"),
                "NCT_Link": st.column_config.LinkColumn("CTG", display_text="Open", width="small"),
                "EnrollmentCount": st.column_config.NumberColumn("Enrollment", format="%d"),
                "BriefSummary": st.column_config.TextColumn("Summary", width="large"),
                "OfficialTitle": st.column_config.TextColumn("Title", width="large"),
            })

    # ── Tab 2: 智能研报生成器 ─────────────────────────────────────────────
    with tab2:
        _company_row = df_summary[df_summary["Symbol"] == selected_symbol].iloc[0] if selected_symbol and not df_summary[df_summary["Symbol"] == selected_symbol].empty else None
        _mcap = _company_row.get("Market Cap") if _company_row is not None else None
        _cash = _company_row.get("Total Cash") if _company_row is not None else None
        _pipeline_rows = pd.DataFrame()
        if not df_master.empty and "Symbol" in df_master.columns:
            _pipeline_rows = df_master.loc[df_master["Symbol"].astype(str).str.strip().str.upper() == selected_symbol.upper()].copy()
        if not _pipeline_rows.empty and "Asset_Name" in _pipeline_rows.columns:
            _pipeline_rows = _pipeline_rows[~_pipeline_rows["Asset_Name"].apply(lambda x: _is_placebo_asset(x) if pd.notna(x) else False)]
        _dd_prompt = _build_dd_prompt(selected_symbol, company_name or "", _mcap or 0, _cash or 0, _pipeline_rows)
        st.markdown("**🧠 终极 DD Prompt（复制到 Gemini）**")
        st.text_area("Prompt（可全选 Ctrl+A 后复制）", value=_dd_prompt, height=400, key="dd_prompt_ta")
        st.caption("将上述 Prompt 复制到 Gemini、Claude 或 Deep Research 工具中生成研报，然后上传至下方。")
        _report_upload = st.file_uploader("上传 Gemini 生成的研报 (支持 .md / .txt / .gdoc / .doc / .docx)", type=["md", "txt", "gdoc", "doc", "docx"], key="report_uploader")
        if _report_upload is not None and selected_symbol:
            if st.button("💾 保存研报", type="primary", key="save_report_btn"):
                _dd_dir = _ensure_ai_dd_report_dir(selected_symbol)
                _ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                _name = (_report_upload.name or "").lower()
                if _name.endswith(".docx"):
                    _ext = ".docx"
                elif _name.endswith(".doc"):
                    _ext = ".doc"
                elif _name.endswith(".gdoc"):
                    _ext = ".gdoc"
                elif _name.endswith(".txt"):
                    _ext = ".txt"
                else:
                    _ext = ".md"
                _save_name = f"Report_{_ts}{_ext}"
                _save_path = _dd_dir / _save_name
                if _ext in (".doc", ".docx"):
                    _save_path.write_bytes(_report_upload.getvalue())
                else:
                    _save_path.write_text(_report_upload.getvalue().decode("utf-8", errors="replace"), encoding="utf-8")
                st.success(f"研报已保存至 {_save_path.relative_to(DATA_DIR)}")
                st.rerun()

    # ── Tab 3: 研报阅读与投研笔记 ─────────────────────────────────────────────
    with tab3:
        _report_files = _list_report_files(selected_symbol)
        if not _report_files:
            st.info("尚无该公司的历史研报，请先在 Tab 2 中生成并上传。")
        else:
            _opts = [f"{human} - {fname}" for fname, human in _report_files]
            _sel = st.selectbox("选择历史研报", options=_opts, key="report_selector")
            if _sel:
                _sel_fname = _sel.split(" - ", 1)[1] if " - " in _sel else _report_files[0][0]
                _report_path = AI_DD_REPORT_DIR / selected_symbol.upper() / _sel_fname
                _report_content = ""
                _docx_error = None  # .docx 解析失败时的错误信息
                if _report_path.exists():
                    _suffix = _report_path.suffix.lower()
                    if _suffix == ".docx":
                        _report_content, _docx_error = _extract_docx_text(_report_path)
                    elif _suffix == ".doc":
                        _report_content = ""  # .doc 不解析，仅提供下载
                    else:
                        _report_content = _report_path.read_text(encoding="utf-8", errors="replace")
                # .gdoc 为 Google Docs 快捷方式，内容为 JSON 含 url
                _is_gdoc = _report_path.suffix.lower() == ".gdoc"
                _is_doc = _report_path.suffix.lower() == ".doc"
                _is_docx = _report_path.suffix.lower() == ".docx"
                # 笔记：每家公司共用一份永久笔记 Notes_{TICKER}.md，无论切换哪篇研报都加载同一文件
                _company_notes_fname = f"Notes_{selected_symbol.upper()}.md"
                _company_notes_path = AI_DD_REPORT_DIR / selected_symbol.upper() / _company_notes_fname
                _company_notes_path.parent.mkdir(parents=True, exist_ok=True)
                # 从磁盘读取（每次渲染重新读，保证不同标签页间同步）
                _notes_content = ""
                if _company_notes_path.exists():
                    _notes_content = _company_notes_path.read_text(encoding="utf-8", errors="replace")
                # AI Summary：按了再 Summary，不按不调用
                _summary_key = f"ai_summary_{_sel_fname}"
                if st.button("🤖 再 Summary", key=f"ai_summary_btn_{_sel_fname}", help="调用 Gemini Flash 对当前研报生成简明总结"):
                    with st.spinner("正在调用 Gemini 生成总结…"):
                        _sum, _sum_err = _gemini_summarize_report(_report_content)
                    if _sum_err:
                        st.error(f"总结生成失败：{_sum_err}")
                    elif _sum:
                        st.session_state[_summary_key] = _sum
                        st.success("AI 总结已生成。")
                if st.session_state.get(_summary_key):
                    with st.expander("📋 AI 总结", expanded=True):
                        st.markdown(st.session_state[_summary_key])
                col1, col2 = st.columns([2, 1])
                with col1:
                    st.markdown("**📄 研报内容**")
                    if _is_gdoc:
                        _gdoc_url = None
                        try:
                            _gdoc_data = json.loads(_report_content)
                            if isinstance(_gdoc_data, dict):
                                _gdoc_url = _gdoc_data.get("url")
                        except (json.JSONDecodeError, TypeError):
                            pass
                        if _gdoc_url:
                            st.link_button("📄 在 Google Docs 中打开", _gdoc_url)
                            st.caption("此条目为 Google Docs 链接，请点击上方按钮查看全文。AI 总结仍可对已打开的文档内容手动粘贴后再用。")
                        else:
                            st.code(_report_content, language="json")
                    elif _is_doc:
                        _doc_bytes = _report_path.read_bytes()
                        st.download_button("📥 下载 .doc 文件并用 Word 打开", data=_doc_bytes, file_name=_report_path.name, mime="application/msword", key=f"dl_doc_{_sel_fname}")
                        st.caption(".doc 格式无法在此直接预览，请下载后用 Microsoft Word 打开。")
                    elif _is_docx:
                        if _report_content.strip():
                            try:
                                _report_container = st.container(height=600)
                                with _report_container:
                                    st.markdown(_report_content)
                            except TypeError:
                                st.markdown(_report_content)
                        else:
                            if _docx_error:
                                st.warning(f"解析失败: {_docx_error}")
                            st.download_button("📥 下载 .docx 文件", data=_report_path.read_bytes(), file_name=_report_path.name, mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document", key=f"dl_docx_{_sel_fname}")
                            st.caption("无法在此解析该 .docx 正文。若文件来自网页/邮件另存为，请用 Word 打开后「另存为」标准 .docx 再重新上传；或直接下载后用 Word 查看。")
                    else:
                        try:
                            _report_container = st.container(height=600)
                            with _report_container:
                                st.markdown(_report_content)
                        except TypeError:
                            st.markdown(_report_content)
                with col2:
                    st.markdown(f"**✍️ 我的投研笔记 — {selected_symbol.upper()}**")
                    st.caption(f"文件：{_company_notes_fname}（切换研报不影响笔记，持续累积）")
                    _notes_ta = st.text_area(
                        "随手记录（可持续追加）",
                        value=_notes_content,
                        height=500,
                        key=f"notes_ta_{selected_symbol}",
                    )
                    if st.button("💾 保存笔记", type="primary", key=f"save_notes_{selected_symbol}"):
                        _company_notes_path.write_text(_notes_ta, encoding="utf-8")
                        st.success("笔记已保存。")
                        st.rerun()

# ── Footer ───────────────────────────────────────────────────────

st.markdown("<div class='footer'>Dashboard by Tony Jiang</div>", unsafe_allow_html=True)
