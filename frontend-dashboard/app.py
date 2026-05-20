import streamlit as st
import pandas as pd
from cassandra.cluster import Cluster
import time

# 1. Optimizimi: Konfigurimi i faqes për të zënë të gjithë ekranin
st.set_page_config(page_title="Kosovo Weather IoT", page_icon="🇽🇰", layout="wide")

# 2. Optimizimi: Caching i lidhjes së databazës (nuk lidhet nga e para çdo sekondë)
@st.cache_resource
def init_connection():
    cluster = Cluster(['127.0.0.1'])
    return cluster.connect('weather_ks')

session = init_connection()

st.title("🇽🇰 Monitorimi i Motit në Kohë Reale - Kosovë")

# --- Krijimi i Shiritit Anësor (Sidebar) për Filtrime ---
st.sidebar.header("⚙️ Filtrat e Dashboard-it")
te_gjitha_qytetet = ['Prishtina', 'Prizren', 'Peja', 'Gjakova', 'Mitrovica', 'Gjilan', 'Ferizaj']

# Multi-select widget për të zgjedhur qytetet (sipas parazgjedhjes tregon të gjitha)
qytetet_e_zgjedhura = st.sidebar.multiselect(
    "Zgjidh Qytetet për Monitorim:", 
    options=te_gjitha_qytetet, 
    default=te_gjitha_qytetet
)

# Krijimi i një hapësire dinamike
placeholder = st.empty()

# Looop për rifreskim të të dhënave
while True:
    # Tërheqja e 500 rekordeve të fundit për t'i pasur në memorie
    rows = session.execute("SELECT * FROM sensor_data LIMIT 500")
    df = pd.DataFrame(list(rows))
    
    with placeholder.container():
        if not df.empty and len(qytetet_e_zgjedhura) > 0:
            # Riemërtimi i kolonës për ndërfaqen
            df = df.rename(columns={'sensor_id': 'Qyteti'})
            
            # FILTRIMI: Mbajmë vetëm qytetet që janë zgjedhur në sidebar
            filtered_df = df[df['Qyteti'].isin(qytetet_e_zgjedhura)]
            filtered_df = filtered_df.sort_values(by="timestamp")
            
            # --- Vizualizimet ---
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🌡️ Trendi i Temperaturave")
                # Riformatimi i të dhënave për të shfaqur linja të ndara për çdo qytet
                temp_chart_data = filtered_df.pivot(index='timestamp', columns='Qyteti', values='temperature')
                st.line_chart(temp_chart_data)
                
            with col2:
                st.subheader("💧 Trendi i Lagështisë")
                hum_chart_data = filtered_df.pivot(index='timestamp', columns='Qyteti', values='humidity')
                st.line_chart(hum_chart_data)
                
            st.markdown("---")
            st.subheader("Të Dhënat e Detajuara (Tabela)")
            # Fshehim index-in e Pandas për një pamje më të pastër
            st.dataframe(filtered_df.tail(10).reset_index(drop=True), use_container_width=True)
            
        elif len(qytetet_e_zgjedhura) == 0:
            st.warning("⚠️ Ju lutem zgjidhni të paktën një qytet nga menyja anësore.")
        else:
            st.info("⏳ Duke pritur për të dhëna nga sistemi Kafka/Spark...")
            
    time.sleep(2)