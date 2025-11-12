# app/streamlit_app.py
import os
import streamlit as st
import requests

API = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="Adaptive Index Advisor", layout="wide")
st.title("Adaptive Index Advisor — Demo")

# Create tabs for different views
tab1, tab2 = st.tabs(["Recommendations", "Current Indexes"])

with tab1:
    st.markdown("Fetch recommendations and optionally apply an index (force required).")
    st.info("⚠️ Note: Only 3 indexes can exist at a time. Creating a new index will automatically delete the oldest one.")

    if st.button("Refresh recommendations"):
        st.rerun()

    try:
        recs = requests.get(f"{API}/recommendations").json()
    except Exception as e:
        st.error(f"Error fetching recommendations: {e}")
        recs = []

    if not recs:
        st.info("No recommendations available. Run collector and recommender first.")
    else:
        import pandas as pd
        df = pd.DataFrame(recs)
        st.dataframe(df[['table','column','calls','avg_time_ms','index_exists','recommend']].head(200))

        selected = st.selectbox("Select recommendation to inspect", options=df.index, format_func=lambda i: f"{df.loc[i,'table']}.{df.loc[i,'column']} (calls={df.loc[i,'calls']})")
        if selected is not None:
            r = df.loc[selected]
            st.write("Sample query:")
            st.code(r['sample_query'])
            st.write("Details:")
            st.json(r.to_dict())

            st.write("---")
            if st.checkbox("Force apply this index (use with caution)"):
                username = st.text_input("User name", value="dev")
                if st.button("Apply index now"):
                    payload = {"table": r['table'], "column": r['column'], "force": True, "user": username}
                    try:
                        res = requests.post(f"{API}/apply", json=payload)
                        response_data = res.json()
                        if "deleted_index" in response_data:
                            st.warning(f"⚠️ Deleted oldest index: {response_data['deleted_index']} (to maintain 3 index limit)")
                        st.success(f"✅ Index created: {response_data.get('index')}")
                        st.json(response_data)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Apply failed: {e}")

with tab2:
    st.markdown("## Current Indexes")
    st.markdown("View all indexes currently created by the system (max 3 at a time).")

    if st.button("Refresh indexes"):
        st.rerun()

    try:
        indexes = requests.get(f"{API}/indexes").json()
    except Exception as e:
        st.error(f"Error fetching indexes: {e}")
        indexes = []

    if not indexes:
        st.info("No indexes found. Apply recommendations to create indexes.")
    else:
        import pandas as pd
        from datetime import datetime
        
        # Convert to DataFrame for better display
        df_indexes = pd.DataFrame(indexes)
        
        # Show count
        st.metric("Total Indexes", len(indexes), delta=None)
        
        # Display as table
        if 'created_at' in df_indexes.columns:
            df_indexes['created_at'] = pd.to_datetime(df_indexes['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        st.dataframe(
            df_indexes[['index_name', 'table_name', 'column_name', 'created_at', 'user_name', 'size']],
            use_container_width=True
        )
        
        # Show details for each index
        st.markdown("### Index Details")
        for idx in indexes:
            with st.expander(f"📊 {idx['index_name']}"):
                st.write(f"**Table:** {idx['table_name']}")
                st.write(f"**Column:** {idx['column_name']}")
                st.write(f"**Created:** {idx.get('created_at', 'N/A')}")
                st.write(f"**Created by:** {idx.get('user_name', 'N/A')}")
                st.write(f"**Size:** {idx.get('size', 'N/A')}")
