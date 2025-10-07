# dashboard_postgres.py
import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go

# PostgreSQL configuration
POSTGRES_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "username",
    "password": "password",
    "port": 5432
}

def get_db_connection():
    """Create connection to PostgreSQL"""
    return psycopg2.connect(**POSTGRES_CONFIG)

def get_warehouse_metrics():
    """Fetch latest metrics from PostgreSQL"""
    conn = get_db_connection()
    
    query = """
    SELECT 
        window_start,
        window_end,
        event_type,
        warehouse_id,
        event_count,
        total_quantity,
        avg_quantity
    FROM warehouse_metrics 
    WHERE window_start >= NOW() - INTERVAL '1 hour'
    ORDER BY window_start DESC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_summary_metrics():
    """Get summary metrics for the dashboard"""
    conn = get_db_connection()
    
    summary_query = """
    SELECT 
        COUNT(*) as total_events,
        SUM(total_quantity) as total_quantity,
        AVG(avg_quantity) as overall_avg_quantity,
        COUNT(DISTINCT warehouse_id) as active_warehouses,
        COUNT(DISTINCT event_type) as event_types
    FROM warehouse_metrics 
    WHERE window_start >= NOW() - INTERVAL '1 hour'
    """
    
    cursor = conn.cursor()
    cursor.execute(summary_query)
    result = cursor.fetchone()
    conn.close()
    
    return {
        'total_events': result[0] or 0,
        'total_quantity': result[1] or 0,
        'overall_avg_quantity': result[2] or 0,
        'active_warehouses': result[3] or 0,
        'event_types': result[4] or 0
    }

# Streamlit Dashboard
st.set_page_config(page_title="Warehouse Analytics", layout="wide")
st.title("🏭 Real-time Warehouse Analytics Dashboard")

# Auto-refresh configuration
auto_refresh = st.sidebar.checkbox("Auto Refresh", value=True)
refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 5, 60, 10)

# Time range filter
time_range = st.sidebar.selectbox(
    "Time Range",
    ["Last 15 minutes", "Last 1 hour", "Last 4 hours", "Last 24 hours"]
)

# Main dashboard content
placeholder = st.empty()

def update_dashboard():
    with placeholder.container():
        # Get data
        df = get_warehouse_metrics()
        summary = get_summary_metrics()
        
        if df.empty:
            st.info("📊 Waiting for data to populate...")
            return
        
        # Summary Metrics
        st.subheader("📈 Real-time Summary")
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("Total Events", f"{summary['total_events']:,}")
        with col2:
            st.metric("Total Quantity", f"{summary['total_quantity']:,}")
        with col3:
            st.metric("Avg Qty/Event", f"{summary['overall_avg_quantity']:.2f}")
        with col4:
            st.metric("Active Warehouses", summary['active_warehouses'])
        with col5:
            st.metric("Event Types", summary['event_types'])
        
        # Charts
        col_left, col_right = st.columns(2)
        
        with col_left:
            st.subheader("📊 Events by Type")
            event_summary = df.groupby('event_type')['event_count'].sum().reset_index()
            fig_event = px.pie(event_summary, values='event_count', names='event_type')
            st.plotly_chart(fig_event, use_container_width=True)
        
        with col_right:
            st.subheader("🏭 Warehouse Performance")
            warehouse_summary = df.groupby('warehouse_id')['total_quantity'].sum().reset_index()
            fig_warehouse = px.bar(warehouse_summary, x='warehouse_id', y='total_quantity')
            st.plotly_chart(fig_warehouse, use_container_width=True)
        
        # Time series chart
        st.subheader("⏱️ Events Over Time")
        df_time = df.groupby('window_start')['event_count'].sum().reset_index()
        fig_time = px.line(df_time, x='window_start', y='event_count')
        st.plotly_chart(fig_time, use_container_width=True)
        
        # Raw data table
        st.subheader("📋 Recent Data")
        st.dataframe(df.head(20), use_container_width=True)
        
        # Data freshness
        latest_update = df['window_start'].max() if not df.empty else 'No data'
        st.caption(f"Last update: {latest_update}")

# Initial render
update_dashboard()

# Auto-refresh logic
if auto_refresh:
    import time
    while True:
        time.sleep(refresh_interval)
        update_dashboard()
else:
    if st.button("🔄 Refresh Now"):
        update_dashboard()