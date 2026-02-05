import streamlit as st
from auth_helper import require_login
from datetime import timedelta

require_login()
st.title("Account Details")
st.write("Welcome to the 2030 District data hub. Building data is refreshed weekly.")
st.write("Access a list of all the buildings in your portfolio here. Check to make sure none of your buildings are missing meter data.")

conn = st.connection("sql", type="sql")

# excluded espmid, 865 entries for total portfolio in 
df = conn.query("SELECT TOP (1000) [espmid],[buildingname],[sqfootage],[usetype], [occupancy], [numbuildings] FROM [dbo].[ESPMFIRSTTEST];")

gaps = {}

for espmid in df['espmid']:
    meter_query = f"""
        SELECT [meterid], [startdate], [enddate]
        FROM [dbo].[electric]
        WHERE [espmid] = '{espmid}'
        ORDER BY [startdate]
    """
    
    try:
        meter_df = conn.query(meter_query)
        
        if len(meter_df) <= 1:
            # No gaps possible with 0 or 1 record
            gaps[espmid] = []
            continue
        
        # Convert to datetime - this is necessary
        meter_df['startdate'] = pd.to_datetime(meter_df['startdate'])
        meter_df['enddate'] = pd.to_datetime(meter_df['enddate'])
        
        # No need to sort again since SQL already ordered it
        # meter_df is already sorted by startdate from the SQL query
        
        espmid_gaps = []  # List to store all gaps for this espmid
        
        # Check for gaps between consecutive meter periods
        for i in range(len(meter_df) - 1):
            current_end = meter_df.loc[i, 'enddate']
            next_start = meter_df.loc[i + 1, 'startdate']
            
            # If there's a gap between the end of current and start of next
            if next_start > current_end + timedelta(days=1):
                gap_start = current_end + timedelta(days=1)
                gap_end = next_start - timedelta(days=1)
                
                # Add this gap to the list for this espmid
                espmid_gaps.append({
                    'gap_start': gap_start,
                    'gap_end': gap_end
                })
        
        # Store all gaps for this espmid in the main dictionary
        gaps[espmid] = espmid_gaps
        
    except Exception as e:
        print(f"Error processing espmid {espmid}: {str(e)}")
        gaps[espmid] = []
    
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

# Check if any gaps exist
if any(gaps.values()):  # Check if any espmid has gaps
    for espmid, gap_list in gaps.items():
        if gap_list:  # Only show espmids with gaps
            for gap in gap_list:
                st.error(f"ESPM ID {espmid}: Gap from {gap['gap_start'].date()} to {gap['gap_end'].date()}")
else:
    st.success("No gaps found in meter data!")