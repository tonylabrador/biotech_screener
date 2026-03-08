import pathlib
import pandas as pd
import streamlit as st

st.set_page_config(page_title="纯生物制药标的分析台", layout="wide")

# Reduce default padding so the table fills more screen space
st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 0rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("纯生物制药标的分析台 (非肿瘤)")

# ── 加载数据 ──────────────────────────────────────────────────

DATA_DIR = pathlib.Path(__file__).parent
CSV_PATH = DATA_DIR / "Final_Non_Oncology_Pharma.csv"

NUMERIC_COLS = [
    "Price", "Market Cap", "EV", "Total Debt", "Total Cash",
    "52W Low", "52W High", "Wall Street Ratings",
    "Shares Outstanding", "Institutional Shares", "Insider %",
]


@st.cache_data(show_spinner="正在加载数据…")
def load_data() -> pd.DataFrame:
    if not CSV_PATH.exists():
        return pd.DataFrame()

    df = pd.read_csv(CSV_PATH)

    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


df_all = load_data()

if df_all.empty:
    st.error(
        f"未找到数据文件 **{CSV_PATH.name}**，"
        "请先运行 clean_biotech.py 生成清洗后的名单。"
    )
    st.stop()

# ── 默认值 ────────────────────────────────────────────────────

mcap_min_b = df_all["Market Cap"].min() / 1e9
mcap_max_b = df_all["Market Cap"].max() / 1e9

HAS_RATINGS = "Wall Street Ratings" in df_all.columns
if HAS_RATINGS:
    _rating_col = df_all["Wall Street Ratings"].dropna()
    RATING_LO_DEFAULT = float(_rating_col.min()) if not _rating_col.empty else 1.0
    RATING_HI_DEFAULT = float(_rating_col.max()) if not _rating_col.empty else 5.0

DEFAULTS = {
    "search_text": "",
    "mcap_lo": round(mcap_min_b, 2),
    "mcap_hi": round(mcap_max_b, 2),
    "rating_range": (RATING_LO_DEFAULT, RATING_HI_DEFAULT) if HAS_RATINGS else (1.0, 5.0),
    "include_no_rating": True,
    "do_search": False,
}

for k, v in DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v


def _reset_filters():
    for k, v in DEFAULTS.items():
        st.session_state[k] = v


def _execute_search():
    st.session_state["do_search"] = True


# ── 侧边栏过滤器 ──────────────────────────────────────────────

st.sidebar.header("筛选条件")

# 1) 文本搜索
search_text = st.sidebar.text_input(
    "🔍 关键词搜索（公司名 / 代码 / 简介）", key="search_text",
)

# 2) 市值区间 — number_input
st.sidebar.markdown("**市值区间 (Billion USD)**")
sb_col1, sb_col2 = st.sidebar.columns(2)
with sb_col1:
    mcap_lo = st.number_input(
        "最低 ($B)", min_value=0.0, max_value=mcap_max_b,
        step=0.1, format="%.2f", key="mcap_lo",
    )
with sb_col2:
    mcap_hi = st.number_input(
        "最高 ($B)", min_value=0.0, max_value=mcap_max_b * 1.5,
        step=0.1, format="%.2f", key="mcap_hi",
    )

# 3) 评级 — slider
if HAS_RATINGS:
    rating_range = st.sidebar.slider(
        "华尔街评级区间",
        min_value=RATING_LO_DEFAULT,
        max_value=RATING_HI_DEFAULT,
        step=0.1,
        format="%.1f",
        key="rating_range",
    )
    include_no_rating = st.sidebar.checkbox("包含无评级公司", key="include_no_rating")
else:
    rating_range = None
    include_no_rating = True

# 4) 操作按钮
st.sidebar.markdown("---")
btn_col1, btn_col2 = st.sidebar.columns(2)
with btn_col1:
    st.button("🔎 执行筛选", on_click=_execute_search, use_container_width=True)
with btn_col2:
    st.button("↩️ 还原", on_click=_reset_filters, use_container_width=True)

# ── 应用过滤 ──────────────────────────────────────────────────

if st.session_state["do_search"]:
    filtered = df_all.copy()

    if search_text:
        kw = search_text.lower()
        text_mask = (
            filtered["Name"].str.lower().str.contains(kw, na=False)
            | filtered["Symbol"].str.lower().str.contains(kw, na=False)
            | filtered["Business Summary"].str.lower().str.contains(kw, na=False)
        )
        filtered = filtered[text_mask]

    filtered = filtered[
        (filtered["Market Cap"] >= mcap_lo * 1e9)
        & (filtered["Market Cap"] <= mcap_hi * 1e9)
    ]

    if rating_range is not None:
        if include_no_rating:
            r_mask = filtered["Wall Street Ratings"].isna() | (
                (filtered["Wall Street Ratings"] >= rating_range[0])
                & (filtered["Wall Street Ratings"] <= rating_range[1])
            )
        else:
            r_mask = (
                (filtered["Wall Street Ratings"] >= rating_range[0])
                & (filtered["Wall Street Ratings"] <= rating_range[1])
            )
        filtered = filtered[r_mask]
else:
    filtered = df_all.copy()

# ── KPI 看板 ──────────────────────────────────────────────────

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("筛选公司总数", f"{len(filtered)} 家")
with col2:
    avg_mcap = filtered["Market Cap"].mean()
    st.metric(
        "平均市值",
        f"${avg_mcap / 1e9:.2f}B" if pd.notna(avg_mcap) else "N/A",
    )
with col3:
    avg_cash = filtered["Total Cash"].mean()
    st.metric(
        "平均现金储备",
        f"${avg_cash / 1e6:.1f}M" if pd.notna(avg_cash) else "N/A",
    )

# ── 构建显示列 & 单位换算 ─────────────────────────────────────

display = filtered.copy()

# Billion 列
for col in ["Market Cap", "EV"]:
    if col in display.columns:
        display[col] = display[col] / 1e9

# Million 列
for col in ["Total Debt", "Total Cash"]:
    if col in display.columns:
        display[col] = display[col] / 1e6

ORDERED_COLS = [
    "Symbol", "Name", "Price", "Country", "Industry",
    "Market Cap", "EV",
    "Wall Street Ratings",
    "52W Low", "52W High",
    "Shares Outstanding", "Institutional Shares", "Insider %",
    "Total Debt", "Total Cash",
    "Business Summary",
]
display_cols = [c for c in ORDERED_COLS if c in display.columns]

st.subheader("公司列表")

st.dataframe(
    display[display_cols].reset_index(drop=True),
    use_container_width=True,
    hide_index=True,
    height=700,
    column_config={
        "Market Cap": st.column_config.NumberColumn("Mkt Cap ($B)", format="%.2f"),
        "EV": st.column_config.NumberColumn("EV ($B)", format="%.2f"),
        "Total Debt": st.column_config.NumberColumn("Debt ($M)", format="%.1f"),
        "Total Cash": st.column_config.NumberColumn("Cash ($M)", format="%.1f"),
        "Price": st.column_config.NumberColumn("Price", format="$%.2f"),
        "52W Low": st.column_config.NumberColumn("52W Low", format="$%.2f"),
        "52W High": st.column_config.NumberColumn("52W High", format="$%.2f"),
        "Wall Street Ratings": st.column_config.NumberColumn(
            "WS Rating", format="%.1f"
        ),
        "Insider %": st.column_config.NumberColumn("Insider %", format="%.2f%%"),
        "Shares Outstanding": st.column_config.NumberColumn("Shares Out", format="%d"),
        "Institutional Shares": st.column_config.NumberColumn("Inst Shares", format="%d"),
        "Business Summary": st.column_config.TextColumn(
            "Business Summary", width="large"
        ),
    },
)
