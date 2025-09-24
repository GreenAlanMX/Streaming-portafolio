
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Page configuration for mobile responsiveness
st.set_page_config(
    page_title="Streaming Platform Analytics Dashboard",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load data
@st.cache_data
def load_data():
    return pd.read_csv('output/user_aggregation_with_clusters.csv')

df = load_data()

# Sidebar filters
st.sidebar.header(" Filters")
selected_countries = st.sidebar.multiselect(
    "Select Countries",
    options=df['country'].unique(),
    default=df['country'].unique()[:5]
)

selected_subscriptions = st.sidebar.multiselect(
    "Select Subscription Types",
    options=df['subscription_type'].unique(),
    default=df['subscription_type'].unique()
)

selected_clusters = st.sidebar.multiselect(
    "Select User Clusters",
    options=sorted(df['cluster_kmeans'].unique()),
    default=sorted(df['cluster_kmeans'].unique())
)

# Filter data
filtered_df = df[
    (df['country'].isin(selected_countries)) &
    (df['subscription_type'].isin(selected_subscriptions)) &
    (df['cluster_kmeans'].isin(selected_clusters))
]

# Main dashboard
st.title(" Streaming Platform Analytics Dashboard")
st.markdown("Real-time insights into user behavior, content performance, and business metrics")

# KPI Cards (Mobile responsive)
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Total Users",
        value=f"{len(filtered_df):,}",
        delta=f"{len(filtered_df) - len(df):,}"
    )

with col2:
    total_sessions = filtered_df['sessions_count'].sum()
    st.metric(
        label="Total Sessions",
        value=f"{total_sessions:,}",
        delta=f"{total_sessions - df['sessions_count'].sum():,}"
    )

with col3:
    retention_rate = (filtered_df['retained'].sum() / len(filtered_df)) * 100
    st.metric(
        label="Retention Rate",
        value=f"{retention_rate:.1f}%",
        delta=f"{retention_rate - (df['retained'].sum() / len(df)) * 100:.1f}%"
    )

with col4:
    avg_duration = filtered_df['avg_duration'].mean()
    st.metric(
        label="Avg Session Duration",
        value=f"{avg_duration:.1f} min",
        delta=f"{avg_duration - df['avg_duration'].mean():.1f} min"
    )

# Charts (Mobile responsive)
col1, col2 = st.columns(2)

with col1:
    st.subheader("User Distribution by Country")
    country_counts = filtered_df['country'].value_counts().head(10)
    fig = px.bar(
        x=country_counts.index,
        y=country_counts.values,
        title="Users by Country"
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Subscription Type Distribution")
    subscription_counts = filtered_df['subscription_type'].value_counts()
    fig = px.pie(
        values=subscription_counts.values,
        names=subscription_counts.index,
        title="Subscription Types"
    )
    st.plotly_chart(fig, use_container_width=True)

# User Segmentation Analysis
st.subheader("User Segmentation Analysis")
col1, col2 = st.columns(2)

with col1:
    cluster_counts = filtered_df['cluster_kmeans'].value_counts()
    fig = px.bar(
        x=[f'Cluster {i}' for i in cluster_counts.index],
        y=cluster_counts.values,
        title="User Clusters Distribution"
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.box(
        filtered_df,
        x='cluster_kmeans',
        y='age',
        title="Age Distribution by Cluster"
    )
    st.plotly_chart(fig, use_container_width=True)

# Content Performance
st.subheader("Content Performance Metrics")
col1, col2 = st.columns(2)

with col1:
    fig = px.scatter(
        filtered_df,
        x='sessions_count',
        y='avg_duration',
        color='subscription_type',
        size='avg_completion',
        title="Sessions vs Duration (bubble size = completion rate)"
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.histogram(
        filtered_df,
        x='avg_completion',
        nbins=20,
        title="Completion Rate Distribution"
    )
    st.plotly_chart(fig, use_container_width=True)

# Geographic Analysis
st.subheader("Geographic Performance Analysis")
country_metrics = filtered_df.groupby('country').agg({
    'sessions_count': 'sum',
    'avg_duration': 'mean',
    'retained': 'mean',
    'subscription_type': lambda x: (x == 'Premium').mean()
}).round(3)

st.dataframe(country_metrics, use_container_width=True)
