import streamlit as st
from auth_helper import require_login

require_login()
st.title("Building Data")
st.write("Content for the Building Data page goes here.")