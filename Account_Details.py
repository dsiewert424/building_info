import streamlit as st
from auth_helper import require_login

require_login()
st.title("Account Details")
st.write("Home page content here.")

# conn = st.connection("sql", type="sql")

# 2. Run a test query
# df = conn.query("SELECT TOP (10) [entryid];")