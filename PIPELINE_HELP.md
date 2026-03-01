# Biotech Non-Oncology Pipeline — 完整文档

## 项目概述

本项目从 Seeking Alpha 导出的 XLSX 文件出发，经过公司筛选、临床试验抓取、数据补全、管线聚合和 AI 治疗领域归类，最终输出一张以药物管线为维度的全景分析表。

---

## 一、环境配置

### 依赖安装

```bash
pip install -r requirements.txt
```

`requirements.txt` 包含：

| 包名 | 用途 |
|------|------|
| streamlit | 可视化仪表盘 |
| yfinance | 抓取公司行业与简介 |
| pandas | 数据处理 |
| openpyxl | 读取 xlsx（备用引擎） |
| python-calamine | 读取 xlsx（主引擎，更稳定） |
| tqdm | 终端进度条 |
| google-genai | Gemini AI SDK |
| python-dotenv | 从 .env 读取环境变量 |

### API Key 配置

在项目根目录创建 `.env` 文件：

```
GEMINI_API_KEY=你的_Gemini_API_Key
```

---

## 二、Pipeline 流程

### 流程图

```
XLSX files (Seeking Alpha 导出)
    │
    ▼
┌──────────────────────────────────────────────┐
│  Step 1: clean_biotech.py                    │
│  合并 XLSX → yfinance 补充 → 双重过滤        │
│  输出: Final_Non_Oncology_Pharma.csv         │
└──────────────────┬───────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────┐
│  Step 2: fetch_trials.py                     │
│  CTG API 三重防线搜索 + Gemini AI 别名识别    │
│  输出: Raw_Clinical_Trials.csv               │
└──────────────────┬───────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────┐
│  Step 3: enrich_trials.py                    │
│  按 NCTId 逐条回查 CTG API 补全详细字段       │
│  输出: Enriched_Clinical_Trials.csv          │
└──────────────────┬───────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────┐
│  Step 4: build_pipeline.py                   │
│  药物名清洗 → 按 Asset 聚合 → Gemini TA 归类  │
│  输出: Biotech_Pipeline_Master.csv           │
└──────────────────┬───────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────┐
│  Step 5: post_process (run_pipeline.py 内置)  │
│  Oncology 二次筛查 + 无试验公司列表            │
│  输出: Oncology_Pipelines.csv                │
│        Companies_No_Active_Trials.csv        │
└──────────────────────────────────────────────┘
```

### 运行方式

```bash
# 全量重跑（从 xlsx 开始，约 1-2 小时）
python run_pipeline.py

# 从指定步骤开始（跳过前面已完成的步骤）
python run_pipeline.py --from step2    # 跳过 yfinance
python run_pipeline.py --from step3    # 跳过试验抓取
python run_pipeline.py --from step4    # 仅重跑管线聚合 + TA 归类
```

---

## 三、各步骤详解

### Step 1: `clean_biotech.py`

**用途：** 合并 Seeking Alpha XLSX 导出文件，用 yfinance 补充行业信息，过滤掉非制药和肿瘤公司。

**输入文件：**
- `all pharma_biotech_1 2026-02-26.xlsx`
- `all pharma_biotech_2 2026-02-26.xlsx`
- `all pharma_biotech_3 2026-02-26.xlsx`

> 注意：更新数据时需修改脚本中的文件名，或将新文件重命名为以上格式。

**处理逻辑：**
1. 用 `python-calamine` 引擎读取三个 XLSX，按行拼接
2. 按 `Symbol` 去重
3. 遍历每家公司，从 yfinance 获取 `industry` 和 `longBusinessSummary`
4. 双重过滤：
   - **条件 A（非制药）：** industry 含 Medical Devices/Diagnostics 等，或 summary 含 medical device/CRO 等关键词
   - **条件 B（肿瘤）：** summary 含 oncology/cancer/tumor/CAR-T 等关键词

**耗时：** 约 30-45 分钟（取决于 yfinance 响应速度）

---

### Step 2: `fetch_trials.py`

**用途：** 从 ClinicalTrials.gov API v2 批量抓取每家公司的活跃临床试验。

**搜索策略（三重防线）：**

| 防线 | 方法 | 说明 |
|------|------|------|
| Tier 1 | 正则清洗 + 多变体查询 | 去掉 Inc/Ltd/A/S 等后缀，生成 2-4 个查询词依次尝试 |
| Tier 2 | Gemini AI 别名识别 | 调用 gemini-2.5-flash，让 AI 识别公司的 CTG sponsor 名或子公司 |
| Tier 3 | 首词兜底 | 用公司名第一个单词作为最后搜索手段 |

**状态过滤：** 仅保留 `RECRUITING`, `ACTIVE_NOT_RECRUITING`, `NOT_YET_RECRUITING`

**耗时：** 约 15-25 分钟

---

### Step 3: `enrich_trials.py`

**用途：** 用已有的 NCTId 列表回查 CTG API，补全每条试验的详细元数据。

**补全逻辑：** 逐条 `GET /api/v2/studies/{NCTId}` 调用，解析 JSON 提取 24 个额外字段。

**容错：** 每 100 条保存 checkpoint，中断后可续跑。

**耗时：** 约 35-40 分钟（取决于试验总数）

---

### Step 4: `build_pipeline.py`

**用途：** 将试验级数据透视为药物管线级全景表，并用 Gemini AI 归类治疗领域。

**处理逻辑：**
1. 清洗药物名称：去掉 `DRUG:`/`BIOLOGICAL:` 前缀、剂量后缀、过滤 Placebo
2. 展开：一条试验可能有多个药物 → 拆成多行
3. 按 `(Symbol, Company_Name, Asset_Name)` 聚合：提取最高 Phase、试验数、入组总人数、适应症合并
4. 批量调用 Gemini（每批 20 条）进行治疗领域归类

**Phase 优先级：** `PHASE4 > PHASE3 > PHASE2 > PHASE1 > EARLY_PHASE1`

**治疗领域标准池：**
Immunology/Autoimmune, Neurology/CNS, Metabolic/Endocrinology, Cardiovascular, Infectious Diseases, Rare/Orphan Diseases, Ophthalmology, Respiratory, Dermatology, Gastroenterology, Hematology, Musculoskeletal, Urology/Nephrology, Psychiatry, Pain/Analgesia, Others

**耗时：** 约 5-10 分钟

---

### Step 5: Post-process（`run_pipeline.py` 内置）

**用途：** Oncology 二次筛查 + 生成辅助文件。

**Oncology 二次筛查规则：**
- 若一家公司的 oncology 管线占比 >30% 或绝对数量 >=10 → 从所有文件中整体移除
- 若占比 <=30% 且数量 <10 → 仅从 Pipeline Master 中移除其 oncology 行，公司本身保留

---

## 四、输出文件格式

### 1. `Final_Non_Oncology_Pharma.csv`

**描述：** 经过清洗和过滤后的纯非肿瘤生物制药公司名单。Dashboard (`app.py`) 的数据源。

| 列名 | 类型 | 说明 |
|------|------|------|
| Symbol | str | 股票代码 |
| Price | float | 当前股价 |
| Name | str | 公司全名 |
| Country | str | 注册国家 |
| 52W Low | float | 52 周最低价 |
| 52W High | float | 52 周最高价 |
| Wall Street Ratings | float | 华尔街评级（1-5 分） |
| Market Cap | float | 市值（美元） |
| EV | float | 企业价值（美元） |
| Shares Outstanding | float | 流通股数 |
| Institutional Shares | float | 机构持股比例 |
| Insider % | float | 内部人持股比例 |
| Total Debt | float | 总负债（美元） |
| Total Cash | float | 现金储备（美元） |
| Industry | str | yfinance 返回的行业分类 |
| Business Summary | str | yfinance 返回的公司简介 |

---

### 2. `Raw_Clinical_Trials.csv`

**描述：** 从 CTG API 抓取的活跃临床试验原始数据（精简字段）。

| 列名 | 类型 | 说明 |
|------|------|------|
| Symbol | str | 股票代码 |
| Company_Name | str | 公司全名 |
| NCTId | str | ClinicalTrials.gov 编号（如 NCT05126758） |
| Phases | str | 试验阶段（PHASE1, PHASE2, PHASE3 等） |
| Status | str | 试验状态（RECRUITING 等） |
| Conditions | str | 适应症，逗号分隔 |
| Interventions | str | 药物/干预措施名称，逗号分隔 |

---

### 3. `Enriched_Clinical_Trials.csv`

**描述：** 在 Raw 基础上补全的完整试验元数据（31 列）。

原始 7 列 + 以下 24 个补全字段：

| 列名 | 类型 | 说明 |
|------|------|------|
| Acronym | str | 试验简称（如 HOPE-3, VITESSE） |
| Allocation | str | 分配方式（RANDOMIZED / NA） |
| ArmCount | int | 试验组数 |
| ArmLabels | str | 各组标签，竖线分隔 |
| ArmTypes | str | 各组类型（EXPERIMENTAL / PLACEBO_COMPARATOR），竖线分隔 |
| BriefSummary | str | 试验简介（截取前 500 字） |
| Collaborators | str | 合作机构，逗号分隔 |
| CompletionDate | str | 整体完成日期 |
| CompletionDateType | str | ACTUAL / ESTIMATED |
| EligibleAges | str | 年龄范围（如 "10 Years - N/A"） |
| EnrollmentCount | int | 样本量（入组人数） |
| EnrollmentType | str | ACTUAL（实际）/ ESTIMATED（预计） |
| InterventionModel | str | 干预模型（PARALLEL / SINGLE_GROUP 等） |
| LeadSponsorClass | str | 申办方类型（INDUSTRY / NIH / OTHER） |
| Masking | str | 盲法级别（QUADRUPLE / TRIPLE / DOUBLE / NONE） |
| OfficialTitle | str | 试验正式标题 |
| PrimaryCompletionDate | str | 主要终点完成日期 |
| PrimaryCompletionDateType | str | ACTUAL / ESTIMATED |
| PrimaryOutcomeMeasures | str | 主要终点指标，竖线分隔 |
| PrimaryPurpose | str | 主要目的（TREATMENT / PREVENTION 等） |
| Sex | str | 性别要求（ALL / MALE / FEMALE） |
| StartDate | str | 试验开始日期 |
| StartDateType | str | ACTUAL / ESTIMATED |
| StudyType | str | 研究类型（INTERVENTIONAL / OBSERVATIONAL） |

---

### 4. `Biotech_Pipeline_Master.csv` ⭐ 核心输出

**描述：** 以药物管线为维度的全景分析表。每行代表一家公司的一个药物资产。

| 列名 | 类型 | 说明 |
|------|------|------|
| Symbol | str | 股票代码 |
| Company_Name | str | 公司全名 |
| Asset_Name | str | 药物/资产名称（已清洗，去除 Placebo） |
| Highest_Phase | str | 该药物的最高研发阶段（PHASE3 > PHASE2 > PHASE1…） |
| Active_Trial_Count | int | 该药物当前活跃试验数 |
| Total_Enrollment | int | 该药物所有试验的入组人数总和 |
| Therapeutic_Area | str | Gemini AI 归类的治疗领域（1-2 个） |
| Detailed_Conditions | str | 所有试验的去重适应症，分号分隔 |
| Trial_NCTIds | str | 对应的 NCTId 列表，逗号分隔 |

**排序规则：** Highest_Phase 降序 → Active_Trial_Count 降序

---

### 5. `Companies_No_Active_Trials.csv`

**描述：** 在 CTG 上未找到任何活跃试验的公司列表。

| 列名 | 类型 | 说明 |
|------|------|------|
| Symbol | str | 股票代码 |
| Name | str | 公司全名 |
| Market Cap | float | 市值（美元） |
| Industry | str | 行业分类 |

---

### 6. `Oncology_Pipelines.csv`（参考文件）

**描述：** Oncology 二次筛查时识别出的肿瘤管线记录。格式与 `Biotech_Pipeline_Master.csv` 完全相同。用于审计和参考，不参与后续分析。

---

## 五、定期更新流程

当你拿到新的 Seeking Alpha XLSX 导出文件时：

1. **替换 XLSX 文件**
   将新文件放入项目根目录。如果文件名变了，修改 `clean_biotech.py` 中的 `FILES` 列表。

2. **一键全量重跑**
   ```bash
   python run_pipeline.py
   ```

3. **查看结果**
   ```bash
   # 若提示 streamlit 找不到，请用：
   python -m streamlit run app.py
   python -m streamlit run dashboard.py

   # 公司名单 + 财务筛选（Final_Non_Oncology_Pharma）
   python -m streamlit run app.py

   # 管线与试验审查（Company_Pipeline_Summary + Enriched_Clinical_Trials）
   python -m streamlit run dashboard.py

   # 或直接查看 CSV
   # Biotech_Pipeline_Master.csv  → 管线全景
   # Company_Pipeline_Summary.csv → 公司×治疗领域汇总（供 dashboard 使用）
   # Companies_No_Active_Trials.csv → 无试验公司
   ```

### 部分重跑场景

| 场景 | 命令 | 说明 |
|------|------|------|
| XLSX 文件更新了 | `python run_pipeline.py` | 全量重跑 |
| 只想重新抓试验（公司名单没变） | `python run_pipeline.py --from step2` | 跳过 yfinance |
| 只想重新补全试验详情 | `python run_pipeline.py --from step3` | 跳过试验抓取 |
| 只想重新归类 TA | `python run_pipeline.py --from step4` | 仅重跑聚合 + AI |

---

## 六、文件依赖关系

```
xlsx files
  └─> Final_Non_Oncology_Pharma.csv  (Step 1)
        └─> Raw_Clinical_Trials.csv  (Step 2)
              └─> Enriched_Clinical_Trials.csv  (Step 3)
                    └─> Biotech_Pipeline_Master.csv  (Step 4)
                          ├─> Oncology_Pipelines.csv  (Step 5)
                          └─> Companies_No_Active_Trials.csv  (Step 5)
```

每个步骤只依赖上一步的输出文件，因此可以从任意中间步骤开始重跑。

---

## 七、分享 Dashboard 给他人

若希望朋友通过浏览器直接打开你的 Dashboard（无需安装 Python），可选方式如下。

### 1. Streamlit Community Cloud（推荐，免费）

- 网址：<https://share.streamlit.io>
- 用 GitHub 登录，从仓库部署；每次 push 可自动更新。
- **注意**：需把 `dashboard.py` 和依赖的 CSV（`Company_Pipeline_Summary.csv`、`Enriched_Clinical_Trials.csv`）一并放入仓库，或通过「Resources」上传数据文件，这样云端才能读到数据。
- 免费额度有限制（带宽/运行时间），适合个人或小范围分享。
- **详细步骤与需要上传的文件清单见：[DEPLOY_GITHUB.md](DEPLOY_GITHUB.md)**

### 2. Hugging Face Spaces

- 网址：<https://huggingface.co/spaces>
- 新建 Space，选择 **Streamlit** 模板，上传 `dashboard.py`、`requirements.txt` 和数据 CSV。
- 生成公开链接（如 `https://huggingface.co/spaces/你的用户名/biotech-dashboard`），可分享给朋友。

### 3. 本机临时分享（ngrok / localtunnel）

- 在你本机运行：`python -m streamlit run dashboard.py`
- 用 [ngrok](https://ngrok.com) 或 [localtunnel](https://localtunnel.github.io/www/) 把本机端口（Streamlit 默认 8501）暴露为一个公网 URL。
- 朋友通过该 URL 访问；**仅在你电脑开机且命令未关闭时有效**，适合临时演示。

### 4. 自建服务器（VPS / Render / Railway 等）

- 将项目部署到云主机或 PaaS（如 Render、Railway），配置 Python 环境并运行 `streamlit run dashboard.py --server.port 8080`。
- 把 CSV 放在同目录或可访问路径，即可通过你提供的域名或 URL 分享。

### 5. 仅分享数据 + 说明

- 若朋友有 Python 环境，可只分享 `Company_Pipeline_Summary.csv`、`Enriched_Clinical_Trials.csv` 和 `dashboard.py`（及 `requirements.txt`），对方本地运行：
  ```bash
  pip install -r requirements.txt
  python -m streamlit run dashboard.py
  ```

**小结**：要给别人一个「打开链接即用」的页面，优先考虑 **Streamlit Community Cloud** 或 **Hugging Face Spaces**，并确保部署时包含上述两个 CSV 文件。
