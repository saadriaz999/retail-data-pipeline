import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import altair as alt

st.set_page_config(layout="wide")

st.title("Retail Sales Dashboard")

session = get_active_session()

# -------------------------
# FILTERS
# -------------------------

years_df = session.sql("""
SELECT DISTINCT year
FROM fact_sales
WHERE year IS NOT NULL
ORDER BY year
""").to_pandas()

years = [int(y) for y in years_df["YEAR"].dropna().tolist()]

if len(years) == 0:
    st.error("No year data available in fact_sales table.")
    st.stop()

selected_year = st.sidebar.selectbox("Select Year", years)

months_df = session.sql(f"""
SELECT DISTINCT month
FROM fact_sales
WHERE year = {selected_year}
AND month IS NOT NULL
ORDER BY month
""").to_pandas()

months = [int(m) for m in months_df["MONTH"].dropna().tolist()]

if len(months) == 0:
    st.error("No month data available for selected year.")
    st.stop()

selected_month = st.sidebar.selectbox("Select Month", months)

# -------------------------
# KPI METRICS
# -------------------------

kpi_query = f"""
SELECT
    SUM(total_amount) AS revenue,
    SUM(quantity) AS total_items,
    COUNT(DISTINCT order_id) AS orders
FROM fact_sales
WHERE year = {selected_year}
AND month = {selected_month}
"""

kpi_df = session.sql(kpi_query).to_pandas()

col1, col2, col3 = st.columns(3)

revenue = kpi_df["REVENUE"][0] if kpi_df["REVENUE"][0] is not None else 0
items = kpi_df["TOTAL_ITEMS"][0] if kpi_df["TOTAL_ITEMS"][0] is not None else 0
orders = kpi_df["ORDERS"][0] if kpi_df["ORDERS"][0] is not None else 0

col1.metric("Total Revenue", f"${revenue:,.2f}")
col2.metric("Total Items Sold", int(items))
col3.metric("Orders", int(orders))

# -------------------------
# REVENUE TREND
# -------------------------

st.subheader("Revenue Trend")

trend_query = f"""
SELECT date, SUM(total_amount) revenue
FROM fact_sales
WHERE year = {selected_year}
AND month = {selected_month}
GROUP BY date
ORDER BY date
"""

trend_df = session.sql(trend_query).to_pandas()

if not trend_df.empty:
    chart = alt.Chart(trend_df).mark_line().encode(
        x=alt.X("DATE", title="Date"),
        y=alt.Y("REVENUE", title="Revenue ($)"),
    ).configure_legend(disable=True)
    st.altair_chart(chart, use_container_width=True)

# -------------------------
# TOP PRODUCTS
# -------------------------

st.subheader("Top Products")

product_query = f"""
SELECT
    p.product_name,
    SUM(f.total_amount) revenue
FROM fact_sales f
JOIN dim_product p
ON f.product_id = p.product_id
WHERE f.year = {selected_year}
AND f.month = {selected_month}
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 10
"""

product_df = session.sql(product_query).to_pandas()

if not product_df.empty:
    chart = alt.Chart(product_df).mark_bar().encode(
        x=alt.X("PRODUCT_NAME", title="Product", sort="-y"),
        y=alt.Y("REVENUE", title="Revenue ($)"),
    ).configure_legend(disable=True)
    st.altair_chart(chart, use_container_width=True)

# -------------------------
# STORE PERFORMANCE
# -------------------------

st.subheader("Top Stores")

store_query = f"""
SELECT
    s.store_name,
    SUM(f.total_amount) revenue
FROM fact_sales f
JOIN dim_store s
ON f.store_id = s.store_id
WHERE f.year = {selected_year}
AND f.month = {selected_month}
GROUP BY s.store_name
ORDER BY revenue DESC
LIMIT 10
"""

store_df = session.sql(store_query).to_pandas()

if not store_df.empty:
    chart = alt.Chart(store_df).mark_bar().encode(
        x=alt.X("STORE_NAME", title="Store", sort="-y"),
        y=alt.Y("REVENUE", title="Revenue ($)"),
    ).configure_legend(disable=True)
    st.altair_chart(chart, use_container_width=True)

# -------------------------
# PAYMENT METHODS
# -------------------------

st.subheader("Payment Method Revenue")

payment_query = f"""
SELECT payment_method, SUM(total_amount) revenue
FROM fact_sales
WHERE year = {selected_year}
AND month = {selected_month}
GROUP BY payment_method
ORDER BY revenue DESC
"""

payment_df = session.sql(payment_query).to_pandas()

if not payment_df.empty:
    chart = alt.Chart(payment_df).mark_bar().encode(
        x=alt.X("PAYMENT_METHOD", title="Payment Method", sort="-y"),
        y=alt.Y("REVENUE", title="Revenue ($)"),
    ).configure_legend(disable=True)
    st.altair_chart(chart, use_container_width=True)

# -------------------------
# CUSTOMER COUNTRIES
# -------------------------

st.subheader("Customer Countries")

country_query = """
SELECT c.country, COUNT(*) customers
FROM dim_customer c
GROUP BY c.country
ORDER BY customers DESC
"""

country_df = session.sql(country_query).to_pandas()

if not country_df.empty:
    chart = alt.Chart(country_df).mark_bar().encode(
        x=alt.X("COUNTRY", title="Country", sort="-y"),
        y=alt.Y("CUSTOMERS", title="Customers"),
    ).configure_legend(disable=True)
    st.altair_chart(chart, use_container_width=True)