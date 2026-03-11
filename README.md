# 🧬 Biotech Intelligence Dashboard — Non-Oncology Focus

A personal investment research platform for tracking, analyzing, and managing non-oncology biotech companies. Built with Streamlit and powered by yfinance, Google Gemini, and a custom clinical trial enrichment pipeline.

**Live App:** [Streamlit Community Cloud](https://share.streamlit.io) *(link to be updated)*

---

## ✨ Features Overview

### 📊 Tab 1 — 公司筛选与研报 (Company Screener & Research)

**Company Screener Table**
- Displays all non-oncology biotech companies with key financials: Price, Upside %, Market Cap, EV, Pipeline Count, Active Trials, Next Catalyst, 52W High/Low, Cash, Debt
- **📄 Latest Report column** — shows the date of the most recent DD report uploaded for each company
- Sort by any column header; filter to a single company via dropdown

**Sidebar Filters** (applied on "Execute Search")
- Market Cap range (B USD)
- Therapeutic Area (multi-select, Oncology excluded)
- Phase of Development
- Has Marketed Drug (Yes / No / All)
- **Has DD Report in System** (Yes / No / All) — filter companies by whether a research report has been uploaded
- Upside % range (from Wall Street consensus targets)
- Refresh Upside % button (fetches from yfinance, persists to disk across sessions)

**Company Detail Panel** (select a company to expand)

Header shows: Symbol, Company Name, and **latest report date** (or "暂无研报" if none)

Three sub-tabs:
- **📊 管线与临床概览** — macro financials, Wall Street consensus (target high/mean/low, recommendation, analyst count, upside %), associated files, drug pipeline table, clinical trials table
- **🧠 智能研报生成器** — auto-assembles a DD prompt using company financials and pipeline data; one-click copy button
- **📂 研报阅读与投研笔记** — upload AI-generated reports (`.md`, `.txt`, `.docx`, `.doc`, `.rtf`, `.gdoc`), browse historical reports by timestamp, read report content, write and save per-company research notes (`Notes_{TICKER}.md`)

---

### 💼 Tab 2 — 投资组合优化 (Portfolio Optimizer)

- **Step 1** — Scans all uploaded DD reports and notes across all companies to build a unified knowledge archive (`Master_DD_Archive.md`)
- **Step 2** — Configure investment parameters: total capital, investment horizon, company selection, extra risk constraints
- **Step 3** — Click **⚡ 生成资产配置 Prompt** to generate a structured Gemini Deep Research prompt with:
  - 4 portfolio strategies: Defensive, Balanced, Aggressive, M&A Arbitrage
  - Constraints: MoA diversification, cash runway discipline, odds-weighted sizing, **differentiated position sizing per strategy**, **therapeutic area balance (where applicable)**
  - One-click copy button — paste into Gemini Deep Research along with the archive file

---

### 📈 Tab 3 — 策略竞技场 (Strategy Arena & Paper Trading)

A full paper trading and strategy comparison platform.

#### ⚡ A · 批量建仓 & AI 导入

- **Upload or select saved reports** for AI parsing (two sub-tabs: upload new / pick from archive)
- Reports auto-saved to `Paper_Trading_Portfolios/Arena_Reports/` with timestamp
- **AI Batch Parsing** — sends report to Gemini API with a strict JSON extraction prompt; outputs up to 10 portfolio drafts
- **Draft Review Table** — editable `st.data_editor` per strategy with:
  - Auto-fetches latest price (yfinance) for holdings with missing avg cost
  - Auto-calculates integer shares from dollar amount (or vice versa)
  - **💵 CASH and 📊 TOTAL rows** appended in-table — Alloc % calculated against total capital (including cash)
  - Total capital input with auto-derived cash position
  - Validation: blocks deployment if stock value exceeds total capital
  - Bar chart showing allocation including cash
- **Manual Portfolio Creation** — form-based input (`Symbol Shares [AvgPrice]` or `Symbol $Amount`); appends to draft queue
- **Draft Management** — individual ✕ delete buttons + bulk "🗑️ 清空全部草案"
- **⚡ 批量部署至虚拟盘仓库** — fetches live prices, saves all valid strategies as JSON
- **Delete Saved Strategies** section at bottom of Tab A

#### 🏇 B · 多策略赛马对比

- Select up to 10 saved strategies via multiselect
- "🔄 刷新实时净值" button fetches live prices and calculates current portfolio value
- `st.metric` cards: strategy name, initial capital, current value, total P&L %
- Net asset value history chart (`st.line_chart`) with **XBI and SPY benchmarks** normalized to average initial capital
- Holdings detail expander with **💵 CASH and 📊 TOTAL rows**
- Auto-appends daily total value to `portfolio_history.csv`

#### 🔍 C · 历史回溯归因

- Multi-strategy selection (up to 10) + date range picker
- "📊 生成回溯报告" fetches historical prices via `yf.download`
- Multi-curve chart: all selected strategies + **XBI/SPY benchmarks** (normalized)
- `st.metric` for period return per strategy and per benchmark
- Per-strategy attribution table with: start/end price, % change, P&L contribution, **💵 CASH, 📊 TOTAL, 📌 XBI 基准, 📌 SPY 基准** rows

---

## 📁 File & Directory Structure

```
biotech_screener/
├── dashboard.py                  # Main Streamlit app
├── app.py                        # Data pipeline entry point
├── enrich_trials.py              # Clinical trial enrichment
├── requirements.txt
├── .streamlit/
│   ├── config.toml
│   └── secrets.toml.template
├── Company_Pipeline_Summary.csv  # Main data (git-ignored if large)
├── Biotech_Pipeline_Master.csv
├── Enriched_Clinical_Trials.csv
├── upside_cache.json             # Persisted Wall Street upside data
├── associated_files/             # Per-company uploaded files
│   └── {SYMBOL}/
├── AI_DD_REPORT/                 # DD reports and notes
│   └── {SYMBOL}/
│       ├── Report_YYYYMMDD_HHMMSS.md
│       └── Notes_{SYMBOL}.md
└── Paper_Trading_Portfolios/     # Strategy JSON files
    ├── {Strategy_Name}_YYYYMMDD.json
    ├── portfolio_history.csv
    └── Arena_Reports/            # Uploaded strategy research reports
        └── YYYYMMDD_HHMMSS_{filename}.{ext}
```

---

## 🚀 Local Setup

```bash
pip install -r requirements.txt
streamlit run dashboard.py
```

### Environment Variables / Secrets

Create `.streamlit/secrets.toml` (local) or set in Streamlit Cloud dashboard:

```toml
GEMINI_API_KEY = "your-gemini-api-key"
```

Or use a `.env` file locally:

```
GEMINI_API_KEY=your-gemini-api-key
```

---

## ⚠️ Streamlit Cloud — Important Notes

Streamlit Community Cloud is **stateless** — the filesystem resets on each redeploy or restart. This means:

| Data | Persistence |
|------|-------------|
| CSV source files (committed to repo) | ✅ Permanent |
| `upside_cache.json` | ❌ Lost on restart |
| `AI_DD_REPORT/` reports & notes | ❌ Lost on restart |
| `Paper_Trading_Portfolios/` JSONs | ❌ Lost on restart |

**Recommendation:** Use the app primarily for local development where file persistence is guaranteed. For cloud use, re-upload reports and re-deploy strategies after each restart.

---

## 📋 公司白名单（whitelist_symbols.json）

部分非肿瘤公司因 yfinance 的 Business Summary 中**偶然出现**肿瘤相关词汇（如 `tumor`、`oncology`、`leukemia`），会被 `clean_biotech.py` 的自动过滤规则误判为肿瘤公司并剔除。白名单文件可强制保留这些公司。

### 文件位置

```
biotech_screener/whitelist_symbols.json
```

### 文件格式（JSON，便于在 Cursor 中直接编辑）

```json
[
  {
    "symbol": "SYRE",
    "reason": "IBD/rheumatic focus; yfinance summary contains 'tumor' (anti-TNF context) but NOT oncology company"
  },
  {
    "symbol": "SGMT",
    "reason": "NASH/metabolic focus; yfinance summary contains 'oncology' incidentally but NOT oncology company"
  },
  {
    "symbol": "TENX",
    "reason": "Cardiopulmonary focus; yfinance summary contains 'leukemia' incidentally but NOT oncology company"
  }
]
```

| 字段 | 说明 |
|------|------|
| `symbol` | 股票代码（大小写不敏感） |
| `reason` | 备注说明（可选，便于以后查阅原因） |

### 如何添加新公司

1. 打开 `whitelist_symbols.json`，在数组中追加一个新对象，例如：
   ```json
   {
     "symbol": "NEWCO",
     "reason": "Non-oncology company incorrectly flagged"
   }
   ```
2. 注意逗号：最后一个对象前需加逗号，最后一个对象后不加逗号
3. 保存文件
4. **重新运行 Step 1 pipeline**：`python clean_biotech.py`，再运行后续步骤重新生成 CSV 数据

> ⚠️ 白名单只对 `should_drop()` 过滤生效。如果某公司本来就不在原始 xlsx 数据源里，需要先手动添加到 `Final_Non_Oncology_Pharma.csv`。

---

## 🛠 Tech Stack

| Component | Library |
|-----------|---------|
| UI Framework | `streamlit` |
| Financial Data | `yfinance` |
| AI Summarization & Strategy | `google-genai` (Gemini Flash) |
| Document Parsing | `python-docx`, `docx2txt`, `striprtf` |
| Data Processing | `pandas`, `pathlib` |
| Visualization | Streamlit native charts |

---

## 📋 Requirements

See `requirements.txt`. Key dependencies:

```
streamlit
yfinance
pandas
google-genai
python-docx
docx2txt
striprtf
```

---

*Dashboard by **Tony Jiang** · v2.0*
