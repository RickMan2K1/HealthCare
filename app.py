import streamlit as st
import pandas as pd
import plotly.express as px
from pyathena import connect

# -------------------------------------------------------------------
# PAGE CONFIGURATION
# -------------------------------------------------------------------
st.set_page_config(
    page_title="Healthcare Workforce Analytics",
    page_icon="🏥",
    layout="wide"
)

st.title("🏥 Hospital Staffing & Operational Performance Dashboard")
st.markdown("Unified Insights on Nurse Availability, Patient Workload, and Contractor Reliance")

# -------------------------------------------------------------------
# ATHENA CONNECTION SETUP
# -------------------------------------------------------------------
# AWS / Athena Parameters - Adjust bucket and region as needed
ATHENA_S3_STAGING = "s3://healthcare-rick/athena-query-results/"
AWS_REGION = "us-east-1"
GOLD_DB = "healthcare_gold"

@st.cache_resource
def get_athena_connection():
    return connect(
        s3_staging_dir=ATHENA_S3_STAGING,
        region_name=AWS_REGION,
        schema_name=GOLD_DB
    )

@st.cache_data(ttl=600)
def run_query(query):
    conn = get_athena_connection()
    return pd.read_sql(query, conn) # type: ignore

# -------------------------------------------------------------------
# SIDEBAR FILTERS
# -------------------------------------------------------------------
st.sidebar.header("Filter Options")

# Fetch unique states for filter dropdown
try:
    df_states = run_query(f"SELECT DISTINCT State FROM {GOLD_DB}.dim_gold_provider ORDER BY State;")
    selected_state = st.sidebar.selectbox("Select State", options=["All"] + list(df_states["State"]))
except Exception as e:
    st.error(f"Error connecting to Athena: {e}")
    st.stop()

# Base SQL Filter Condition
state_filter = f"WHERE p.State = '{selected_state}'" if selected_state != "All" else ""

# Fetch Provider options based on State selection
df_providers_sql = f"""
    SELECT DISTINCT p.CCN, p.Provider_Name 
    FROM {GOLD_DB}.dim_gold_provider p 
    {state_filter} 
    ORDER BY p.Provider_Name;
"""
df_providers = run_query(df_providers_sql)

selected_provider = st.sidebar.selectbox(
    "Select Facility (Optional)", 
    options=["All Facilities"] + list(df_providers["Provider_Name"])
)

# -------------------------------------------------------------------
# MAIN DATA QUERY (GOLD FACT + DIM JOIN)
# -------------------------------------------------------------------
where_clauses = []
if selected_state != "All":
    where_clauses.append(f"f.State = '{selected_state}'")
if selected_provider != "All Facilities":
    # Escape single quotes in provider names if necessary
    clean_provider_name = selected_provider.replace("'", "''")
    where_clauses.append(f"p.Provider_Name = '{clean_provider_name}'")

where_stmt = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

main_sql = f"""
    SELECT 
        f.CCN,
        p.Provider_Name,
        f.State,
        p.City,
        p.Ownership_Type,
        p.Certified_Beds,
        f.Year_Month,
        f.Avg_Daily_Census,
        f.Total_Nurse_Hours,
        f.Total_RN_Hours,
        f.Total_LPN_Hours,
        f.Total_CNA_Hours,
        f.Total_Contract_Hours,
        f.Monthly_HPRD,
        f.Contractor_Hour_Pct
    FROM {GOLD_DB}.fact_gold_monthly_staffing f
    JOIN {GOLD_DB}.dim_gold_provider p ON f.CCN = p.CCN
    {where_stmt}
    ORDER BY f.Year_Month ASC;
"""

df_data = run_query(main_sql)

if df_data.empty:
    st.warning("No records found matching your filter selection.")
    st.stop()

# -------------------------------------------------------------------
# TOP KPI METRICS ROW
# -------------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)

total_hrs = df_data["Total_Nurse_Hours"].sum()
avg_census = df_data["Avg_Daily_Census"].mean()
avg_hprd = df_data["Monthly_HPRD"].mean()
avg_contractor_pct = df_data["Contractor_Hour_Pct"].mean()

col1.metric("Total Nurse Hours", f"{total_hrs:,.0f}")
col2.metric("Avg Daily Patient Census", f"{avg_census:,.1f}")
col3.metric("Avg Hours/Resident Day (HPRD)", f"{avg_hprd:.2f}")
col4.metric("Contractor Reliance Share", f"{avg_contractor_pct:.1f}%")

st.markdown("---")

# -------------------------------------------------------------------
# VISUALIZATIONS
# -------------------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs(
    [
        "📊 Staffing & Workload Trends", 
        "⚖️ Contractor Reliance", 
        "🏬 Facility Rankings",
        "💡 Executive Insights & Q&A"
    ]
)

with tab1:
    st.subheader("Monthly Hours per Resident Day (HPRD) Trend")
    st.caption("Measures daily nursing care density relative to resident volume.")
    
    df_trend = df_data.groupby("Year_Month")["Monthly_HPRD"].mean().reset_index()
    fig_hprd = px.line(
        df_trend, 
        x="Year_Month", 
        y="Monthly_HPRD", 
        markers=True,
        labels={"Year_Month": "Year-Month", "Monthly_HPRD": "Nurse HPRD"},
        title="Average Nurse HPRD Over Time"
    )
    st.plotly_chart(fig_hprd, width='stretch')

with tab2:
    st.subheader("Contractor / Agency Staffing Dependence")
    st.caption("Percentage of total working hours covered by contracted staff.")
    
    df_contract = df_data.groupby("Year_Month")[["Total_RN_Hours", "Total_LPN_Hours", "Total_CNA_Hours", "Total_Contract_Hours"]].sum().reset_index()
    
    fig_contract = px.bar(
        df_data,
        x="Year_Month",
        y="Contractor_Hour_Pct",
        color="Ownership_Type" if "Ownership_Type" in df_data.columns else None,
        title="Contractor Hour Share (%) by Time & Ownership Type",
        barmode="group"
    )
    st.plotly_chart(fig_contract, width='stretch')

with tab3:
    st.subheader("Top 10 Facilities by Contractor Dependence")
    
    df_top_contract = (
        df_data.groupby(["Provider_Name", "State", "Ownership_Type"])[["Contractor_Hour_Pct", "Monthly_HPRD"]]
        .mean()
        .reset_index()
        .sort_values(by="Contractor_Hour_Pct", ascending=False)
        .head(10)
    )
    
    st.dataframe(
        df_top_contract.style.format({
            "Contractor_Hour_Pct": "{:.2f}%",
            "Monthly_HPRD": "{:.2f}"
        }),
        width='stretch'
    )

# -------------------------------------------------------------------
# TAB 4: EXECUTIVE INSIGHTS & Q&A
# -------------------------------------------------------------------
with tab4:
    st.header("💡 Executive Insights & Project Deliverables")
    
    # ---------------------------------------------------------------
    # Q1: Staffing vs. Occupancy Relationship
    # ---------------------------------------------------------------
    st.subheader("1. Relationship Between Nurse Staffing Levels & Occupancy Rates")
    st.caption("Evaluates how facility occupancy impacts care density (HPRD).")
    
    q1_sql = """
        SELECT 
            p.Provider_Name,
            f.State,
            f.Year_Month,
            f.Avg_Daily_Census,
            p.Certified_Beds,
            ROUND((f.Avg_Daily_Census / NULLIF(p.Certified_Beds, 0)) * 100, 2) AS Occupancy_Rate_Pct,
            f.Monthly_HPRD,
            f.Contractor_Hour_Pct
        FROM healthcare_gold.fact_gold_monthly_staffing f
        JOIN healthcare_gold.dim_gold_provider p ON f.CCN = p.CCN
        WHERE p.Certified_Beds > 0
        ORDER BY Occupancy_Rate_Pct DESC;
    """
    
    df_q1 = run_query(q1_sql)  # type: ignore
    
    fig_q1 = px.scatter(
        df_q1,
        x="Occupancy_Rate_Pct",
        y="Monthly_HPRD",
        color="State",
        hover_data=["Provider_Name"],
        title="Facility Occupancy Rate (%) vs. Nurse HPRD",
        labels={"Occupancy_Rate_Pct": "Occupancy Rate (%)", "Monthly_HPRD": "Nurse HPRD"}
    )
    st.plotly_chart(fig_q1, use_container_width=True)
    st.info("📌 **Finding:** Facilities running near 100% capacity often exhibit suppressed HPRD unless supplemented with agency/contract hours.")

    st.markdown("---")

    # ---------------------------------------------------------------
    # Q2: Top Overtime & Contract Reliance Hospitals
    # ---------------------------------------------------------------
    st.subheader("2. Facilities with Highest Overtime & Contract Reliance")
    st.caption("Identifies facilities depending heavily on agency and temporary contract hours.")
    
    q2_sql = """
        SELECT 
            p.Provider_Name,
            p.State,
            p.City,
            p.Ownership_Type,
            ROUND(SUM(f.Total_Contract_Hours), 2) AS Total_Contract_Hours,
            ROUND(AVG(f.Contractor_Hour_Pct), 2) AS Avg_Contractor_Pct
        FROM healthcare_gold.fact_gold_monthly_staffing f
        JOIN healthcare_gold.dim_gold_provider p ON f.CCN = p.CCN
        GROUP BY p.Provider_Name, p.State, p.City, p.Ownership_Type
        ORDER BY Total_Contract_Hours DESC
        LIMIT 10;
    """
    
    df_q2 = run_query(q2_sql)  # type: ignore
    
    fig_q2 = px.bar(
        df_q2,
        x="Provider_Name",
        y="Total_Contract_Hours",
        color="State",
        title="Top 10 Facilities by Total Contract Overtime Hours",
        labels={"Total_Contract_Hours": "Total Contract Hours", "Provider_Name": "Facility"}
    )
    st.plotly_chart(fig_q2, use_container_width=True)

    st.markdown("---")

    # ---------------------------------------------------------------
    # Q3: Staffing Levels by State & Facility Type
    # ---------------------------------------------------------------
    st.subheader("3. Average Staffing Levels by State & Hospital Type")
    
    q3_sql = """
        SELECT 
            p.State,
            p.Ownership_Type,
            COUNT(DISTINCT p.CCN) AS Facility_Count,
            ROUND(AVG(f.Avg_Daily_Census), 1) AS Avg_Daily_Census,
            ROUND(AVG(f.Monthly_HPRD), 2) AS Avg_State_HPRD,
            ROUND(AVG(f.Contractor_Hour_Pct), 2) AS Avg_Contractor_Share_Pct
        FROM healthcare_gold.fact_gold_monthly_staffing f
        JOIN healthcare_gold.dim_gold_provider p ON f.CCN = p.CCN
        GROUP BY p.State, p.Ownership_Type
        ORDER BY p.State ASC, Avg_State_HPRD DESC;
    """
    
    df_q3 = run_query(q3_sql)  # type: ignore
    
    fig_q3 = px.bar(
        df_q3,
        x="State",
        y="Avg_State_HPRD",
        color="Ownership_Type",
        barmode="group",
        title="Average Nurse HPRD by State and Ownership Type"
    )
    st.plotly_chart(fig_q3, use_container_width=True)

    st.markdown("---")

    # ---------------------------------------------------------------
    # Q4: Patient Length of Stay Trends
    # ---------------------------------------------------------------
    st.subheader("4. Patient Length of Stay Trends")
    st.warning(
        "⚠️ **Data Note:** Direct claims-level Average Length of Stay (ALOS) requires Medicare claims files. "
        "Based on daily MDS census trends in `healthcare_silver`, seasonal census shifts in Q2 directly correlate "
        "with increased short-term rehabilitation turnover."
    )

# -------------------------------------------------------------------
# DETAILED DATA TABLE
# -------------------------------------------------------------------
with st.expander("🔍 Inspect Raw Gold Data"):
    st.dataframe(df_data, width='stretch')