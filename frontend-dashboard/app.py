import streamlit as st
import pandas as pd
from cassandra.cluster import Cluster
import time

st.set_page_config(page_title="Kosovo Weather IoT", page_icon="🇽🇰", layout="wide")

@st.cache_resource
def init_connection():
    cluster = Cluster(['127.0.0.1'])
    return cluster.connect('weather_ks')

session = init_connection()

st.title("🇽🇰 Monitorimi i Motit në Kohë Reale - Kosovë")

st.sidebar.header("⚙️ Filtrat e Dashboard-it")
te_gjitha_qytetet = ['Prishtina', 'Prizren', 'Peja', 'Gjakova', 'Mitrovica', 'Gjilan', 'Ferizaj']
qytetet_e_zgjedhura = st.sidebar.multiselect(
    "Zgjidh Qytetet për Monitorim:", 
    options=te_gjitha_qytetet, 
    default=te_gjitha_qytetet
)

placeholder = st.empty()

while True:
    rows = session.execute("SELECT * FROM sensor_data LIMIT 500")
    df = pd.DataFrame(list(rows))
    
    with placeholder.container():
        if not df.empty and len(qytetet_e_zgjedhura) > 0:
            df = df.rename(columns={'sensor_id': 'Qyteti'})
            
            filtered_df = df[df['Qyteti'].isin(qytetet_e_zgjedhura)]
            filtered_df = filtered_df.sort_values(by="timestamp")
            
            # --- Vizualizimet ---
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🌡️ Trendi i Temperaturave")
                temp_chart_data = filtered_df.pivot(index='timestamp', columns='Qyteti', values='temperature')
                st.line_chart(temp_chart_data)
                
            with col2:
                st.subheader("💧 Trendi i Lagështisë")
                hum_chart_data = filtered_df.pivot(index='timestamp', columns='Qyteti', values='humidity')
                st.line_chart(hum_chart_data)
                
            st.markdown("---")
            st.subheader("Të Dhënat e Detajuara (Tabela)")
            st.dataframe(filtered_df.tail(10).reset_index(drop=True), use_container_width=True)
            
        elif len(qytetet_e_zgjedhura) == 0:
            st.warning("⚠️ Ju lutem zgjidhni të paktën një qytet nga menyja anësore.")
        else:
            st.info("⏳ Duke pritur për të dhëna nga sistemi Kafka/Spark...")
            
    time.sleep(2)