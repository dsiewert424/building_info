import streamlit as st
import pandas as pd
import plotly.express as px
from auth_helper import require_login

require_login()

st.title("Portfolio Data")

conn = st.connection("sql", type="sql")

# Get total square footage for each building type
query = """
    SELECT 
        [usetype],
        COALESCE(SUM(TRY_CAST([sqfootage] AS DECIMAL(10,2))), 0) as total_sqft,
        COUNT(*) as building_count
    FROM [dbo].[ESPMFIRSTTEST]
    GROUP BY [usetype]
    ORDER BY total_sqft DESC
"""

df = conn.query(query)

# Show only top 30 building types in the chart
top_30 = df.head(30)

# Bar Chart - Top 30 only
fig_bar = px.bar(
    top_30,
    x='total_sqft',
    y='usetype',
    orientation='h',
    color_discrete_sequence=['#1f77b4']
)

fig_bar.update_layout(
    height=800,
    xaxis_title="Total Square Feet",
    yaxis_title="Building Type",
    yaxis={'categoryorder': 'total ascending'},
    showlegend=False
)

st.plotly_chart(fig_bar, use_container_width=True)

# Pie Chart - Top 10 with more margin for labels
top_10 = df.head(10)

if len(df) > 10:
    other_sqft = df.iloc[10:]['total_sqft'].sum()
    other_count = df.iloc[10:]['building_count'].sum()
    
    top_10 = pd.concat([
        top_10,
        pd.DataFrame([{
            'usetype': f'Other ({len(df)-10} types)',
            'total_sqft': other_sqft,
            'building_count': other_count
        }])
    ])

fig_pie = px.pie(
    top_10,
    values='total_sqft',
    names='usetype',
    hole=0.3
)

# Increase bottom margin to prevent label cutoff
fig_pie.update_layout(
    height=700,  # Make it taller
    margin=dict(t=50, b=150, l=50, r=50),  # More bottom margin
    showlegend=False
)

# Make labels smaller so they fit better
fig_pie.update_traces(
    textposition='outside',
    textinfo='percent+label',
    textfont_size=12  # Smaller font
)

st.plotly_chart(fig_pie, use_container_width=True)


# Summary stats
col1, col2 = st.columns(2)
with col1:
    st.metric("Total Buildings", f"{df['building_count'].sum():,}")
with col2:
    st.metric("Total Sq Ft", f"{df['total_sqft'].sum():,.0f}")