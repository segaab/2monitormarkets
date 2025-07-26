import streamlit as st

# Navigation
st.set_page_config(page_title="COT + RVol Dashboard", layout="wide")
st.sidebar.title("📊 Dashboard Navigation")
selection = st.sidebar.radio("Select Dashboard", ["🧮 COT Analysis", "📈 RVol Monitor"])

# Run COT Analysis or RVol Monitor based on selection
if selection == "🧮 COT Analysis":
    # Import and run COT analysis
    from cot_analysis import main as cot_main
    cot_main()

elif selection == "📈 RVol Monitor":
    # Import and run RVol dashboard
    from streamlit_rvol_dashboard import main as rvol_main
    rvol_main()
