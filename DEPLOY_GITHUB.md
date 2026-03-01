# 部署指南：GitHub + Streamlit Community Cloud（纯网页操作版）

## 重要说明：文件存储限制

Streamlit Community Cloud 是**无状态云端容器**，每次重启（或 idle 超时后自动 sleep）都会恢复到 Git 仓库的初始状态。这意味着：

| 数据类型 | 本地运行 | Streamlit Cloud |
|---------|---------|-----------------|
| CSV 源数据（`*.csv`） | ✅ 永久 | ✅ 永久（来自 Git） |
| `AI_DD_REPORT/` 研报 & 笔记 | ✅ 永久 | ⚠️ 重启后丢失 |
| `associated_files/` 关联文件 | ✅ 永久 | ⚠️ 重启后丢失 |

**推荐策略**：把现有的 `AI_DD_REPORT/` 和 `associated_files/` 也上传到 GitHub，这样重启后这些初始文件仍在。

---

## Step 1：注册 GitHub 账号（已有账号跳过）

1. 打开 [https://github.com](https://github.com)
2. 点右上角 **Sign up**，按提示注册账号
3. 验证邮箱后登录

---

## Step 2：创建新的 GitHub 仓库

1. 登录 GitHub 后，点右上角 **"+"** → **"New repository"**
2. 填写：
   - **Repository name**：`biotech-screener`（或你喜欢的名字）
   - **Description**：（可选）Biotech Intelligence Dashboard
   - 选 **Private**（私有，推荐，因为含有公司数据）或 Public
   - **不要**勾选 "Add a README file"（保持空仓库）
3. 点 **"Create repository"**
4. 创建后页面会显示一个空仓库，**先不关闭这个页面**

---

## Step 3：安装 GitHub Desktop（图形界面工具）

> GitHub Desktop 是 GitHub 官方出品的桌面应用，完全图形化，不需要用命令行。

1. 打开 [https://desktop.github.com](https://desktop.github.com)
2. 点 **"Download for Windows"**，下载安装包（约 120MB）
3. 安装完成后打开，用你的 **GitHub 账号登录**

---

## Step 4：用 GitHub Desktop 把本地代码上传到 GitHub

### 4.1 添加本地文件夹

1. 打开 GitHub Desktop
2. 点菜单 **File → Add Local Repository**
3. 点 **"Choose..."**，找到并选择 `C:\tony\biotech_screener` 文件夹
4. 如果提示 "This directory does not appear to be a Git repository"，点 **"create a repository"**
5. 在弹出窗口中：
   - Name：`biotech-screener`
   - Local Path：`C:\tony\biotech_screener`（已自动填好）
   - **不要**勾选 "Initialize this repository with a README"
   - 点 **"Create Repository"**

### 4.2 选择要上传的文件

1. 左侧面板会列出所有变更文件，**勾选以下文件**（在 Changes 标签页）：
   - `dashboard.py`
   - `requirements.txt`
   - `build_pipeline.py`
   - `normalize_ta_csvs.py`
   - `run_pipeline.py`、`update_financials.py`、`fetch_trials.py`、`enrich_trials.py`
   - `clean_biotech.py`、`add_websites.py`
   - `Company_Pipeline_Summary.csv`
   - `Biotech_Pipeline_Master.csv`
   - `Enriched_Clinical_Trials.csv`
   - `.streamlit/config.toml`
   - `.streamlit/secrets.toml.template`
   - `DEPLOY_GITHUB.md`、`PIPELINE_HELP.md`
   - `AI_DD_REPORT/`（整个文件夹，含研报和笔记）
   - `associated_files/`（整个文件夹，含关联文件）
   - `associated_files_index.csv`

2. **确认以下文件没有被勾选**（不要上传）：
   - `.env`（含有 API Key，绝对不能上传！）
   - `.streamlit/secrets.toml`（同上）
   - `__pycache__/`（Python 缓存，不需要）
   - `.xlsx` 原始 Excel 文件（太大，且不需要）

### 4.3 提交（Commit）

1. 左下角 **"Summary"** 填写：`Initial commit: Biotech Screener Dashboard`
2. 点蓝色按钮 **"Commit to main"**

### 4.4 推送到 GitHub

1. 点顶部工具栏的 **"Publish repository"**
2. 弹出窗口中：
   - Name：`biotech-screener`
   - 勾选 **"Keep this code private"**（推荐）
3. 点 **"Publish Repository"**
4. 等待几秒，上传完成后即可在 GitHub 网页上看到你的代码

---

## Step 5：配置 Streamlit Community Cloud

1. 打开 [https://share.streamlit.io](https://share.streamlit.io)
2. 点 **"Sign in with GitHub"**，用你的 GitHub 账号登录
3. 点 **"New app"**
4. 选择 **"Deploy from existing repo"**，填写：
   - **Repository**：选择 `你的用户名/biotech-screener`
   - **Branch**：`main`
   - **Main file path**：`dashboard.py`
5. 展开 **"Advanced settings"**，找到 **"Secrets"** 输入框，粘贴以下内容（把 Key 换成你的真实 Key）：

```
GEMINI_API_KEY = "你的真实Gemini API Key"
```

6. 点 **"Deploy!"**
7. 等待约 1-3 分钟，Streamlit 会自动安装依赖并启动你的应用
8. 部署成功后会得到一个网址，例如：`https://biotech-screener-xxxx.streamlit.app`

---

## Step 6：日后更新代码或数据

每次你在本地修改了 `dashboard.py` 或更新了 CSV 数据，按以下步骤同步到云端：

1. 打开 **GitHub Desktop**
2. 左侧 **Changes** 标签页会显示修改的文件，勾选要更新的文件
3. 左下角 Summary 填写更新说明，如：`Update: 修复XX问题`
4. 点 **"Commit to main"**
5. 点顶部 **"Push origin"**
6. 完成！Streamlit Cloud 会**自动检测到更新并重新部署**（约 1 分钟生效）

---

## Step 7：备份研报和笔记到 GitHub（防止 Cloud 重启后丢失）

由于 Streamlit Cloud 重启后会清空新上传的文件，建议定期把本地的研报和笔记同步到 GitHub：

1. 打开 **GitHub Desktop**
2. Changes 里会看到 `AI_DD_REPORT/` 和 `associated_files/` 下的新文件
3. 勾选这些文件
4. Summary 填：`Backup: sync DD reports and notes`
5. Commit → Push
6. 下次 Cloud 重启后，这些文件仍会在仓库里

---

## 常见问题

**Q: GitHub Desktop 看不到 `.env` 文件？**
A: 这是正常的。`.gitignore` 文件已经把 `.env` 排除了，GitHub Desktop 会自动忽略它，不会上传。这样是安全的。

**Q: Cloud 上 Gemini 总结报错"未配置 GEMINI_API_KEY"？**
A: 进入 [share.streamlit.io](https://share.streamlit.io) → 找到你的 App → 右上角三点菜单 → **Settings → Secrets**，确认 `GEMINI_API_KEY = "..."` 已填写并保存。

**Q: 部署后页面一直 loading 或报错？**
A: 点 App 右下角的 **"Manage app"** → **"Logs"** 查看错误日志，把日志内容发给我即可诊断。

**Q: 想替换掉旧版的 App，保留同一个网址？**
A: 在 [share.streamlit.io](https://share.streamlit.io) 找到旧 App → 右上角三点 → **Delete**，然后用新 repo 重新部署一次即可。或者直接把旧 repo 里的文件替换掉（GitHub Desktop 操作同上），Cloud 会自动更新。

**Q: 文件太大 GitHub 不让上传？**
A: GitHub 单文件限制是 100MB。如果 `.xlsx` 原始文件太大，不传就行（`.xlsx` 已在 `.gitignore` 建议排除）。CSV 一般几 MB，没问题。
