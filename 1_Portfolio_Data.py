import streamlit as st
import pandas as pd
import numpy as np
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

# Summary stats
col1, col2 = st.columns(2)
with col1:
    st.metric("Total Buildings", f"{df['building_count'].sum():,}")
with col2:
    st.metric("Total Sq Ft", f"{df['total_sqft'].sum():,.0f}")

# Show only top 30 building types in the chart

# Table - Show all of the categories just to get a sense. 

st.dataframe(df, height=500, hide_index=True)
# fig_bar = px.bar(
#     df,
#     x='total_sqft',
#     y='usetype',
#     orientation='h',
#     color_discrete_sequence=['#1f77b4']
# )

# fig_bar.update_layout(
#     height=800,
#     xaxis_title="Total Square Feet",
#     yaxis_title="Building Type",
#     yaxis={'categoryorder': 'total ascending'},
#     showlegend=False,
#     title = {
#         'text': "District Property by Square Footage",
#         'font': {'size': 20}
#     }
# )


# st.plotly_chart(fig_bar, use_container_width=True)


# Pie Chart - 4 categories: Commercial,City-Owned,Multi-Unit,Institutional
building_categorization = {
    'Commercial' : 'Bar/Nightclub',
    'Commercial' : 'Bowling Alley',
    'Commercial' : 'Convenience Store without Gas Station',
    'Commercial' : 'Financial Office',
    'Commercial' : 'Fitness Center/Health Club/Gym',
    'Commercial' : 'Food Service',
    'Commercial' : 'Hotel',
    'Commercial' : 'Ice/Curling Rink',
    'Commercial' : 'Mixed Use Property',
    'Commercial' : 'Museum',
    'Commercial' : 'Office',
    'Commercial' : 'Other - Entertainment/Public Assembly',
    'Commercial' : 'Other - Mall',
    'Commercial' : 'Other - Recreation',
    'Commercial' : 'Other - Restaurant/Bar',
    'Commercial' : 'Other - Services',
    'Commercial' : 'Parking',
    'Commercial' : 'Personal Services (Health/Beauty, Dry Cleaning, etc)',
    'Commercial' : 'Restaurant',
    'Commercial' : 'Retail Store',
    'Commercial' : 'Self-Storage Facility',
    'Commercial' : 'Strip Mall',
    'Commercial' : 'Supermarket/Grocery Store',
    'Commercial' : 'Swimming Pool',
    'Commercial' : 'Vehicle Dealership',
    'Commercial' : 'Vehicle Repair Services',
    'Commercial' : 'Wholesale Club/Supercenter',
    'Commercial' : 'Other - Lodging/Residential',
    
    'City-Owned' : 'Courthouse',
    'City-Owned' : 'Fire Station',
    'City-Owned' : 'Library',
    'City-Owned' : 'Police Station',
    'City-Owned' : 'Prison/Incarceration',
    'City-Owned' : 'Drinking Water Treatment & Distribution',
    'City-Owned' : 'Wastewater Treatment Plant',
    'City-Owned' : 'Transportation Terminal/Station',
    'City-Owned' : 'Other - Public Services',
    'City-Owned' : 'Other - Utility',
    
    'Multi-Unit' : 'Multifamily Housing',
    'Multi-Unit' : 'Residence Hall/Dormitory',
    'Multi-Unit' : 'Residential Care Facility',
    'Multi-Unit' : 'Senior Living Community',
    
    'Institutional' : 'Adult Education',
    'Institutional' : 'College/University',
    'Institutional' : 'Community Center and Social Meeting Hall',
    'Institutional' : 'K-12 School',
    'Institutional' : 'Laboratory',
    'Institutional' : 'Medical Office',
    'Institutional' : 'Other - Education',
    'Institutional' : 'Other - Technology/Science',
    'Institutional' : 'Worship Facility',
    'Institutional' : 'Distribution Center',
    'Institutional' : 'Energy/Power Station',
    'Institutional' : 'Manufacturing/Industrial Plant',
    'Institutional' : 'Non-Refrigerated Warehouse',
    'Institutional' : 'Other'
}
display_df = df
display_df['usetype'] = df['usetype'].replace(building_categorization)

fig_pie = px.pie(
    display_df,
    values='total_sqft',
    names='usetype',
    hole=0.3
)

fig_pie.update_layout(
    height=700,  
    margin=dict(t=50, b=150, l=50, r=50),  
    showlegend=False,
    title={
        'text': "Largest Property Types by Square Footage",
        'font': {'size': 20}
    }
)

# Make labels smaller so they fit better
fig_pie.update_traces(
    textposition='outside',
    textinfo='percent+label',
    textfont_size=12  # Smaller font
)

st.plotly_chart(fig_pie, use_container_width=True)



# Manually inserted data, not taken from SQL/Energy Star
buildings_data = {
    "years": [2018, 2019, 2021, 2022, 2023, 2024, 2025],
    "buildings": [25, 36, 99, 274, 415, 1154, 1203]
}

# Create dataframe
df = pd.DataFrame(buildings_data)

# Line graph
fig = px.line(
    df,
    x='years',
    y='buildings',
    markers=True
)
fig.update_layout(
    height=500,
    xaxis_title="Year",
    yaxis_title="Number of Buildings",
    title={
        'text': "Ann Arbor 2030 Buildings By Year",
        'font': {'size': 20}
    }
)
st.plotly_chart(fig, use_container_width=True)

# Manually inserted data, not taken from SQL/Energy Star
sqft_data = {
    "years": [2018, 2019, 2021, 2022, 2023, 2024, 2025],
    "square_footage": [859321, 1023938, 2597722, 9433543, 20125392, 35212329, 39033537]
}

# Create dataframe
df = pd.DataFrame(sqft_data)

# Line graph
fig = px.line(
    df,
    x='years',
    y='square_footage',
    markers=True
)
fig.update_layout(
    height=500,
    xaxis_title="Year",
    yaxis_title="Square Footage",
    title={
        'text': "Ann Arbor 2030 Square Footage By Year",
        'font': {'size': 20}
    }
)
st.plotly_chart(fig, use_container_width=True)

# Hardcoded data
st.subheader("Hardcoded Data from 2025 Annual Report")
eui_data = {
    "years": [2018, 2019, 2021, 2022, 2023, 2024],
    "baseline": [94.5, 78.33, 54.32, 80, 74.14, 64.2],
    "actual": [113.08, 74.15, 50.91, 79.68, 70.3, 63.3],
    "target": [64.3, 53.3, 36.9, 54.4, 50.4, 43.7]
}

# Create dataframe and reshape for Plotly
df = pd.DataFrame(eui_data)
df_melted = df.melt(id_vars=['years'], 
                    value_vars=['baseline', 'actual', 'target'],
                    var_name=' ', 
                    value_name='eui')

fig = px.line(
    df_melted,
    x='years',
    y='eui',
    color=' ',
    markers=True
)

fig.update_layout(
    height=500,
    xaxis_title="Year",
    yaxis_title="EUI (kBTU/sq ft)",
    title={
        'text': "Energy Use Intensity By Year",
        'font': {'size': 20}
    }
)

st.subheader("Data from SQL Database")

# # Function to get meter data - FIXED with correct table names
# def get_meter_data(table_name, espmid, energy_type):
#     query = f"""
#         SELECT 
#             [entryid],
#             [meterid],
#             TRY_CAST([usage] AS FLOAT) as usage,
#             [startdate],
#             [enddate]
#         FROM [dbo].[{table_name}]
#         WHERE [espmid] = '{espmid}'
#         ORDER BY [startdate]
#     """
#     try:
#         df = conn.query(query)
#         if not df.empty:
#             df['energy_type'] = energy_type
#             df['startdate'] = pd.to_datetime(df['startdate'])
#             df['enddate'] = pd.to_datetime(df['enddate'])
#             df['year'] = df['startdate'].dt.year
#         else:
#             # Create empty dataframe WITH the 'year' column
#             df = pd.DataFrame(columns=['entryid', 'meterid', 'usage', 'startdate', 'enddate', 'energy_type', 'year'])
        
#         return df
#     except Exception as e:
#         # Return empty dataframe if table doesn't exist or query fails
#         st.warning(f"Could not query table '{table_name}' for building {espmid}: {str(e)[:100]}...")
#         return pd.DataFrame(columns=['entryid', 'meterid', 'usage', 'startdate', 'enddate', 'energy_type', 'year'])

# # Get all ESPMIDs and their square footage from ESPMFIRSTTEST table
# st.write("### Calculating EUI from Actual Meter Data")

# buildings_query = """
#     SELECT [espmid], TRY_CAST([sqfootage] AS FLOAT) as sqft
#     FROM [dbo].[ESPMFIRSTTEST]
#     WHERE [sqfootage] IS NOT NULL 
#     AND TRY_CAST([sqfootage] AS FLOAT) > 0
# """
# buildings_df = conn.query(buildings_query)

# if buildings_df.empty:
#     st.warning("No buildings found with valid square footage.")
# else:
#     st.write(f"Processing {len(buildings_df)} buildings...")
    
#     # Initialize totals for each year
#     KWH_TO_KBTU = 3.412
#     THERM_TO_KBTU = 100
    
#     # Initialize dictionaries
#     total_energy_by_year = {year: 0 for year in range(2018, 2026)}
#     total_sqft_by_year = {year: 0 for year in range(2018, 2026)}
    
#     # Progress bar for processing
#     progress_bar = st.progress(0)
#     total_buildings = len(buildings_df)
    
#     buildings_processed = 0
#     for idx, building_row in buildings_df.iterrows():
#         espmid = building_row['espmid']
#         sqft = building_row['sqft']
        
#         if sqft > 0:
#             # Get meter data for this building - USING CORRECT TABLE NAMES
#             electric_df = get_meter_data('electric', espmid, 'Electric')
#             gas_df = get_meter_data('naturalgas', espmid, 'Natural Gas')
#             solar_df = get_meter_data('solar', espmid, 'Solar')
            
#             # Process each year
#             for year in range(2018, 2026):
#                 # Check if building has any data this year
#                 has_electric = not electric_df[electric_df['year'] == year].empty
#                 has_gas = not gas_df[gas_df['year'] == year].empty
#                 has_solar = not solar_df[solar_df['year'] == year].empty
                
#                 if has_electric or has_gas or has_solar:
#                     # Add square footage for this year
#                     total_sqft_by_year[year] += sqft
                    
#                     # Process electric
#                     if has_electric:
#                         electric_usage = electric_df[electric_df['year'] == year]['usage'].sum()
#                         total_energy_by_year[year] += electric_usage * KWH_TO_KBTU
                    
#                     # Process gas
#                     if has_gas:
#                         gas_usage = gas_df[gas_df['year'] == year]['usage'].sum()
#                         total_energy_by_year[year] += gas_usage * THERM_TO_KBTU
                    
#                     # Process solar (subtract from total)
#                     if has_solar:
#                         solar_usage = solar_df[solar_df['year'] == year]['usage'].sum()
#                         total_energy_by_year[year] -= solar_usage * KWH_TO_KBTU
            
#             buildings_processed += 1
        
#         # Update progress
#         if (idx + 1) % 10 == 0 or (idx + 1) == total_buildings:
#             progress_bar.progress((idx + 1) / total_buildings)
    
#     progress_bar.empty()
    
#     # Calculate EUI for each year
#     eui_by_year = {}
#     for year in range(2018, 2026):
#         if total_sqft_by_year[year] > 0:
#             eui = total_energy_by_year[year] / total_sqft_by_year[year]
#             eui_by_year[year] = round(eui, 2)
#         else:
#             eui_by_year[year] = None
    
#     # Create EUI data for the line graph
#     valid_years = [year for year in eui_by_year.keys() if eui_by_year[year] is not None]
#     valid_eui = [eui_by_year[year] for year in valid_years]
    
#     st.write(f"Successfully processed {buildings_processed} buildings with meter data.")
    
#     # Show quick stats
#     if valid_years:
#         col1, col2, col3 = st.columns(3)
#         with col1:
#             latest_year = max(valid_years)
#             st.metric(f"Latest EUI ({latest_year})", f"{eui_by_year[latest_year]:.1f}")
#         with col2:
#             st.metric("Years with Data", len(valid_years))
#         with col3:
#             if len(valid_years) > 1:
#                 change = eui_by_year[valid_years[-1]] - eui_by_year[valid_years[-2]]
#                 st.metric("Year-over-Year Change", f"{change:+.1f}")
    
#     # Create the EUI line graph
#     if len(valid_years) > 0:
#         fig = px.line(
#             x=valid_years,
#             y=valid_eui,
#             markers=True,
#             labels={'x': 'Year', 'y': 'EUI (kBTU/sq ft)'}
#         )
        
#         fig.update_layout(
#             height=500,
#             xaxis_title="Year",
#             yaxis_title="EUI (kBTU/sq ft)",
#             title={
#                 'text': "Calculated EUI from Actual Meter Data",
#                 'font': {'size': 20}
#             }
#         )
        
#         # Customize x-axis to show all years 2018-2025
#         fig.update_xaxes(
#             tickmode='array',
#             tickvals=list(range(2018, 2026)),
#             range=[2017.5, 2025.5]
#         )
        
#         # Add data labels
#         for i, (x, y) in enumerate(zip(valid_years, valid_eui)):
#             fig.add_annotation(
#                 x=x,
#                 y=y,
#                 text=str(y),
#                 showarrow=True,
#                 arrowhead=1,
#                 ax=0,
#                 ay=-30
#             )
        
#         st.plotly_chart(fig, use_container_width=True)
        
#         # Show comparison with hardcoded data
#         st.write("#### Comparison with Hardcoded EUI Data")
        
#         # Hardcoded EUI data
#         hardcoded_data = {
#             2018: 94.5, 
#             2019: 78.33, 
#             2021: 54.32, 
#             2022: 80, 
#             2023: 74.14, 
#             2024: 64.2
#         }
        
#         # Create comparison dataframe
#         comparison_years = sorted(set(valid_years + list(hardcoded_data.keys())))
#         comparison_rows = []
        
#         for year in comparison_years:
#             row = {'Year': year}
#             row['Calculated EUI'] = eui_by_year.get(year)
#             row['Hardcoded EUI'] = hardcoded_data.get(year)
#             if row['Calculated EUI'] is not None and row['Hardcoded EUI'] is not None:
#                 row['Difference'] = row['Calculated EUI'] - row['Hardcoded EUI']
#             else:
#                 row['Difference'] = None
#             comparison_rows.append(row)
        
#         comparison_df = pd.DataFrame(comparison_rows)
        
#         # Display comparison
#         st.dataframe(comparison_df.style.format({
#             'Calculated EUI': '{:.2f}',
#             'Hardcoded EUI': '{:.2f}',
#             'Difference': '{:+.2f}'
#         }))
        
#     else:
#         st.warning("No meter data found for any buildings in the years 2018-2025.")


# st.plotly_chart(fig, use_container_width=True)

wui_data = {
    "years": [2021, 2022, 2023, 2024],
    "baseline": [52, 38, 22.4, 30.73],
    "actual": [42, 33.06, 22.91, 27.04],
    "target": [35.36, 25.84, 15.23, 20.90]
}

# Create dataframe and reshape for Plotly
df = pd.DataFrame(wui_data)
df_melted = df.melt(id_vars=['years'], 
                    value_vars=['baseline', 'actual', 'target'],
                    var_name=' ', 
                    value_name='wui')

fig = px.line(
    df_melted,
    x='years',
    y='wui',
    color=' ',
    markers=True
)

fig.update_layout(
    height=500,
    xaxis_title="Year",
    yaxis_title="WUI (gal/sq ft)",
    title={
        'text': "Water Use Intensity By Year",
        'font': {'size': 20}
    }
)
# Debugged issue with x-axis tick marks
fig.update_xaxes(
    tickmode='array',
    tickvals=[2021, 2022, 2023, 2024]
)

st.plotly_chart(fig, use_container_width=True)

emissions_data = {
    "years": [2018, 2019, 2021, 2022, 2023, 2024],
    "baseline": [13.44, 16.73, 11.89, 9.4, 7.57, 6.2],
    "current": [11.66, 13.1, 9.49, 7.5, 6.04, 4.6],
    "yearly_target": [11.56, 13.89, 9.16, 6.96, 5.37, 3.9],
    "target_2030": [6.72, 8.37, 5.95, 4.7, 3.79, 3.1]
}

# Create dataframe and reshape for Plotly
df = pd.DataFrame(emissions_data)
df_melted = df.melt(id_vars=['years'], 
                    value_vars=['baseline', 'current', 'yearly_target', 'target_2030'],
                    var_name=' ', 
                    value_name='emissions')

fig = px.line(
    df_melted,
    x='years',
    y='emissions',
    color=' ',
    markers=True
)

fig.update_layout(
    height=500,
    xaxis_title="Year",
    yaxis_title="Emissions (MT CO2e / sq ft)",
    title={
        'text': "District Carbon Emissions By Square Foot",
        'font': {'size': 20}
    }
)

st.plotly_chart(fig, use_container_width=True)
