import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from auth_helper import require_login

require_login()

st.title("Building Comparison Tool")
st.write("This tool is in progress.")
conn = st.connection("sql", type="sql")

