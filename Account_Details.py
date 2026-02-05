import streamlit as st
from auth_helper import require_login
from datetime import timedelta
import pandas as pd

require_login()
st.title("Account Details")
st.write("Welcome to the 2030 District data hub. Building data is refreshed weekly.")
st.write("Access a list of all the buildings in your portfolio here. Check to make sure none of your buildings are missing meter data.")

conn = st.connection("sql", type="sql")

# excluded espmid, 865 entries for total portfolio in 
df = conn.query("SELECT TOP (1000) [espmid],[buildingname],[sqfootage],[usetype], [occupancy], [numbuildings] FROM [dbo].[ESPMFIRSTTEST];")

    
display_df = df.drop(columns=['espmid'])

df = df.rename(columns={
    'buildingname': 'Building Name',
    'sqfootage': 'Square Footage',
    'usetype': 'Use Type',
    'occupancy': 'Occupancy',
    'numbuildings': 'Number of Buildings'
})

st.dataframe(df, height = 500, hide_index=True)

st.header("Meter Data Gaps Found")

# Get all espmids first
espmids = df['espmid'].tolist()

# Create a single query to get ALL meter data at once
if espmids:
    # Create a comma-separated list for SQL IN clause
    espmid_list = ",".join([f"'{str(espmid)}'" for espmid in espmids])
    
    all_meters_query = f"""
        SELECT [espmid], [meterid], [startdate], [enddate]
        FROM [dbo].[electric]
        WHERE [espmid] IN ({espmid_list})
        ORDER BY [espmid], [startdate]
    """
    
    all_meters_df = conn.query(all_meters_query)
    
    # Group by espmid in Python
    grouped = all_meters_df.groupby('espmid')
    
    gaps = {}
    for espmid, group_df in grouped:
        if len(group_df) <= 1:
            gaps[espmid] = []
            continue
        
        group_df['startdate'] = pd.to_datetime(group_df['startdate'])
        group_df['enddate'] = pd.to_datetime(group_df['enddate'])
        
        espmid_gaps = []
        for i in range(len(group_df) - 1):
            if group_df.iloc[i + 1]['startdate'] > group_df.iloc[i]['enddate'] + timedelta(days=1):
                espmid_gaps.append({
                    'gap_start': group_df.iloc[i]['enddate'] + timedelta(days=1),
                    'gap_end': group_df.iloc[i + 1]['startdate'] - timedelta(days=1)
                })
        
        gaps[espmid] = espmid_gaps

# Check if any gaps exist
if any(gaps.values()):  # Check if any espmid has gaps
    for espmid, gap_list in gaps.items():
        if gap_list:  # Only show espmids with gaps
            for gap in gap_list:
                st.error(f"ESPM ID {espmid}: Gap from {gap['gap_start'].date()} to {gap['gap_end'].date()}")
else:
    st.success("No gaps found in meter data!")