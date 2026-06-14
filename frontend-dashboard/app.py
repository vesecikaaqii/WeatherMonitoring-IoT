"""
app.py
======
Streamlit dashboard for the Weather Monitoring IoT project.

Data source : Cassandra (weather_ks)
Control plane: Simulator FastAPI (start/stop/stress-test)

Pages / sections (selectable from the sidebar):
  1. Overview            - KPIs: sensors, alarms, latency, throughput
  2. Live Readings       - per-city filters, latest table, line charts
  3. Sensor Metadata     - static station info from sensor_metadata table
  4. Alarms              - recent alarms filtered by severity
  5. Simulator Control   - start/stop, normal/stress, sensors, rate, anomaly
  6. Performance         - msg/s + latency charts, processed records
  7. AI / Prediction     - anomaly detection results + temperature forecast

Hosts come from environment variables -> works locally and in Docker.

Run:
    streamlit run frontend-dashboard/app.py
"""

import os
import requests
import pandas as pd
import streamlit as st
from cassandra.cluster import Cluster

# --------------------------------------------------------------------------
# Configuration (environment driven)
# --------------------------------------------------------------------------
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "weather_ks")

# Internal URL used for container-to-container API calls (Docker network).
SIMULATOR_API_URL = os.getenv("SIMULATOR_API_URL", "http://localhost:8000")
# Browser-accessible URL shown to the user (a Windows host cannot resolve the
# internal `simulator-api` hostname). Defaults to the published localhost port.
SIMULATOR_API_PUBLIC_URL = os.getenv("SIMULATOR_API_PUBLIC_URL", "http://localhost:8000/docs")

ALL_CITIES = ["Prishtina", "Prizren", "Peja", "Gjakova", "Mitrovica",
              "Gjilan", "Ferizaj", "Podujeva", "Vushtrri"]

st.set_page_config(page_title="Weather Monitoring IoT - Kosovo",
                   page_icon="🌤️", layout="wide")


# --------------------------------------------------------------------------
# Cassandra connection (cached across reruns)
# --------------------------------------------------------------------------
@st.cache_resource
def get_session():
    cluster = Cluster([CASSANDRA_HOST])
    return cluster.connect(KEYSPACE)


def query_df(cql):
    """Run CQL and return a pandas DataFrame (empty on error)."""
    try:
        session = get_session()
        rows = session.execute(cql)
        return pd.DataFrame(list(rows))
    except Exception as exc:
        st.warning(f"Cassandra query failed: {exc}")
        return pd.DataFrame()


# --------------------------------------------------------------------------
# Simulator API helpers
# --------------------------------------------------------------------------
def api_get(path):
    try:
        return requests.get(f"{SIMULATOR_API_URL}{path}", timeout=3).json()
    except Exception as exc:
        return {"error": str(exc)}


def api_post(path, payload=None):
    try:
        return requests.post(f"{SIMULATOR_API_URL}{path}", json=payload or {}, timeout=5).json()
    except Exception as exc:
        return {"error": str(exc)}


# --------------------------------------------------------------------------
# Sidebar navigation
# --------------------------------------------------------------------------
st.sidebar.title("Weather IoT - Kosovo")
page = st.sidebar.radio(
    "Navigation",
    ["Overview", "Live Readings", "Sensor Metadata", "Alarms",
     "Simulator Control", "Performance", "AI / Prediction"],
)
st.sidebar.caption(f"Cassandra: {CASSANDRA_HOST}")
# Show a browser-clickable docs link (NOT the internal simulator-api hostname).
st.sidebar.markdown(f"Simulator API Docs: [{SIMULATOR_API_PUBLIC_URL}]({SIMULATOR_API_PUBLIC_URL})")
if st.sidebar.button("Refresh data"):
    st.rerun()


# ==========================================================================
# 1. OVERVIEW
# ==========================================================================
def page_overview():
    st.title("Overview")

    meta = query_df("SELECT sensor_id, city, status FROM sensor_metadata")
    latest = query_df("SELECT * FROM latest_readings")
    metrics = api_get("/metrics")

    total_sensors = len(meta) if not meta.empty else len(ALL_CITIES)
    active_sensors = int((meta["status"] == "active").sum()) if not meta.empty else 0

    # Count active alarms across all cities
    alarms = query_df("SELECT severity, status FROM alarms")
    active_alarms = 0
    if not alarms.empty:
        active_alarms = int((alarms["status"] == "active").sum())

    avg_latency = round(latest["latency_ms"].mean(), 1) if not latest.empty and "latency_ms" in latest else 0
    mps = metrics.get("messages_per_second", 0) if isinstance(metrics, dict) else 0

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total sensors", total_sensors)
    c2.metric("Active sensors", active_sensors)
    c3.metric("Latest readings", len(latest))
    c4.metric("Active alarms", active_alarms)
    c5.metric("Avg latency (ms)", avg_latency)
    c6.metric("Msg/s (live)", mps)

    st.markdown("---")
    st.subheader("Latest reading per sensor")
    if not latest.empty:
        st.dataframe(latest.sort_values("sensor_id"), use_container_width=True)
    else:
        st.info("No readings yet. Start the simulator and Spark job.")


# ==========================================================================
# 2. LIVE READINGS
# ==========================================================================
def page_live():
    st.title("Live Readings")

    cities = st.multiselect("Filter cities", ALL_CITIES, default=ALL_CITIES)

    latest = query_df("SELECT * FROM latest_readings")
    if not latest.empty:
        latest = latest[latest["city"].isin(cities)]
        st.subheader("Latest readings")
        st.dataframe(latest.sort_values("city"), use_container_width=True)

    st.markdown("---")
    st.subheader("Historical trends")
    sensor_map = {
        "Prishtina": "PR-001", "Prizren": "PZ-001", "Peja": "PE-001",
        "Gjakova": "GJ-001", "Mitrovica": "MI-001", "Gjilan": "GL-001",
        "Ferizaj": "FE-001", "Podujeva": "PD-001", "Vushtrri": "VU-001",
    }
    pick_city = st.selectbox("Select a city for time-series", cities or ALL_CITIES)
    sid = sensor_map.get(pick_city)

    # Discover which date partitions actually exist for this sensor.
    # (sensor_id, date) is the partition key, so SELECT DISTINCT is efficient.
    today = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    parts = query_df("SELECT DISTINCT sensor_id, date FROM sensor_readings")
    available_dates = []
    if not parts.empty and "date" in parts:
        available_dates = sorted(
            parts.loc[parts["sensor_id"] == sid, "date"].dropna().unique().tolist(),
            reverse=True,
        )

    if available_dates:
        date = st.selectbox("Date partition", available_dates,
                            help="Dates that have data in Cassandra for this sensor")
    else:
        # Fallback: no data discovered yet -> let the user type a date.
        date = st.text_input("Date partition (YYYY-MM-DD)", value=today,
                             help="No stored partitions found yet; defaults to today (UTC)")

    if sid and date:
        hist = query_df(
            f"SELECT * FROM sensor_readings WHERE sensor_id='{sid}' AND date='{date}' LIMIT 500"
        )
        if not hist.empty:
            hist = hist.sort_values("timestamp")
            hist = hist.set_index("timestamp")
            params = ["temperature", "humidity", "pressure", "wind_speed",
                      "rainfall", "air_quality", "battery_level"]
            cols = st.columns(2)
            for i, p in enumerate(params):
                if p in hist:
                    with cols[i % 2]:
                        st.caption(p.replace("_", " ").title())
                        st.line_chart(hist[p])
        else:
            st.info(f"No historical data for {sid} on {date}.")


# ==========================================================================
# 3. ALARMS
# ==========================================================================
def page_alarms():
    st.title("Alarms")

    severity = st.multiselect("Severity", ["INFO", "WARNING", "CRITICAL"],
                              default=["WARNING", "CRITICAL"])
    cities = st.multiselect("Cities", ALL_CITIES, default=ALL_CITIES)

    frames = []
    for city in cities:
        df = query_df(f"SELECT * FROM alarms WHERE city='{city}' LIMIT 100")
        if not df.empty:
            frames.append(df)

    if frames:
        alarms = pd.concat(frames, ignore_index=True)
        if severity:
            alarms = alarms[alarms["severity"].isin(severity)]
        alarms = alarms.sort_values("timestamp", ascending=False)
        st.dataframe(
            alarms[["timestamp", "city", "sensor_id", "parameter", "value",
                    "threshold", "severity", "message", "status"]],
            use_container_width=True,
        )
        st.caption(f"{len(alarms)} alarms shown")
    else:
        st.info("No alarms recorded yet.")


# ==========================================================================
# 4. SIMULATOR CONTROL
# ==========================================================================
def page_simulator():
    st.title("Simulator Control")

    health = api_get("/health")
    if "error" in health:
        st.error(f"Simulator API unreachable: {health['error']}")
    else:
        st.success("Simulator API healthy")
        st.caption(f"API docs: {SIMULATOR_API_PUBLIC_URL}")

    status = api_get("/status")
    running = False
    if isinstance(status, dict) and "error" not in status:
        running = bool(status.get("running"))
        c1, c2, c3 = st.columns(3)
        c1.metric("Running", str(running))
        c2.metric("Messages sent", status.get("messages_sent", 0))
        c3.metric("Msg/s", status.get("messages_per_second", 0))

    # Initialize the form from the simulator's CURRENT config so the UI always
    # reflects the live values (and the new ones after an update).
    cfg = api_get("/config")
    if not isinstance(cfg, dict) or "error" in cfg:
        cfg = {}
    cur_mode = cfg.get("mode", "normal")
    cur_num = int(cfg.get("num_sensors", len(ALL_CITIES)))
    cur_interval = float(cfg.get("interval_seconds", 5.0))
    cur_mps = int(cfg.get("messages_per_second", 0))
    cur_anomaly = float(cfg.get("anomaly_rate", 0.05))
    cur_cities = [c for c in cfg.get("cities", ALL_CITIES) if c in ALL_CITIES] or ALL_CITIES

    st.markdown("---")
    st.subheader("Configuration")
    mode = st.selectbox("Mode", ["normal", "stress"],
                        index=(["normal", "stress"].index(cur_mode) if cur_mode in ("normal", "stress") else 0))
    num_sensors = st.slider("Number of sensors", 1, len(ALL_CITIES),
                            min(max(cur_num, 1), len(ALL_CITIES)))
    interval = st.number_input("Interval (seconds, normal mode)", 0.1, 60.0, cur_interval)
    mps = st.number_input("Messages/sec (stress mode)", 0, 5000, cur_mps)
    anomaly = st.slider("Anomaly rate", 0.0, 1.0, cur_anomaly)
    cities = st.multiselect("Cities", ALL_CITIES, default=cur_cities)

    cc1, cc2, cc3 = st.columns(3)
    if cc1.button("Apply config"):
        # Field names MUST match the API's ConfigUpdate model exactly.
        payload = {
            "mode": mode,
            "num_sensors": int(num_sensors),
            "interval_seconds": float(interval),
            "messages_per_second": int(mps),
            "anomaly_rate": float(anomaly),
            "cities": cities,
        }
        res = api_post("/config", payload)
        if isinstance(res, dict) and res.get("updated"):
            st.success("Configuration updated successfully.")
            # Immediately refresh config + metrics so the UI reflects new values.
            new_cfg = api_get("/config")
            new_metrics = api_get("/metrics")
            col_a, col_b = st.columns(2)
            with col_a:
                st.caption("Current configuration")
                st.json(new_cfg)
            with col_b:
                st.caption("Live metrics")
                st.json(new_metrics)
            # The simulator applies config live on its next tick. Tell the user
            # how to force-apply if they prefer a clean restart.
            if running:
                st.info("Changes are applied live. If a setting doesn't take "
                        "effect, click Stop and Start to apply the new settings.")
        else:
            st.error(f"Failed to update configuration: {res}")
    if cc2.button("Start"):
        st.json(api_post("/start"))
    if cc3.button("Stop"):
        st.json(api_post("/stop"))

    st.markdown("---")
    st.subheader("Stress test")
    s1, s2, s3 = st.columns(3)
    test_mps = s1.number_input("Target msg/s", 100, 5000, 500)
    test_dur = s2.number_input("Duration (s)", 5, 600, 30)
    if s3.button("Run stress test"):
        st.json(api_post("/stress-test", {
            "messages_per_second": int(test_mps),
            "duration_seconds": int(test_dur),
            "anomaly_rate": 0.1,
        }))


# ==========================================================================
# 5. PERFORMANCE
# ==========================================================================
def page_performance():
    st.title("Performance")

    # Display-only labels for each performance mode (logic/query unchanged).
    mode_labels = {
        "stream": "Spark Streaming Metrics",
        "stress": "Stress Test Metrics",
    }
    for mode in ["stream", "stress", "normal"]:
        df = query_df(f"SELECT * FROM performance_metrics WHERE mode='{mode}' LIMIT 200")
        if df.empty:
            continue
        st.subheader(mode_labels.get(mode, f"Mode: {mode}"))
        df = df.sort_values("timestamp")
        idx = df.set_index("timestamp")
        c1, c2 = st.columns(2)
        with c1:
            st.caption("Messages / second")
            if "messages_per_second" in idx:
                st.line_chart(idx["messages_per_second"])
        with c2:
            st.caption("Average latency (ms)")
            if "avg_latency_ms" in idx:
                st.line_chart(idx["avg_latency_ms"])
        total = int(df["messages_processed"].sum()) if "messages_processed" in df else 0
        st.metric(f"Total processed ({mode})", total)
        st.dataframe(df.tail(20), use_container_width=True)
        st.markdown("---")

    live = api_get("/metrics")
    if isinstance(live, dict) and "error" not in live:
        st.subheader("Live producer metrics")
        st.json(live)


# ==========================================================================
# 6. AI / PREDICTION
# ==========================================================================
def page_ai():
    st.title("AI / Prediction")

    st.markdown(
        "**Anomaly detection** flags readings whose multi-parameter pattern is "
        "unusual (IsolationForest, with a statistical fallback). This catches "
        "sensor faults and extreme-weather events the static thresholds miss."
    )

    latest = query_df("SELECT sensor_id, city, temperature, is_anomaly, severity FROM latest_readings")
    if not latest.empty and "is_anomaly" in latest:
        n_anom = int(latest["is_anomaly"].fillna(False).sum())
        st.metric("Sensors flagged anomalous (latest)", n_anom)
        st.dataframe(latest.sort_values("is_anomaly", ascending=False),
                     use_container_width=True)
    else:
        st.info("No readings with anomaly flags yet.")

    st.markdown("---")
    st.subheader("🌡️ Temperature forecast (moving average — LSTM-ready)")
    st.caption(
        "Read-only prediction generated automatically from the latest sensor "
        "readings stored in Cassandra — this value is **not** entered manually "
        "and is **not** random. The active method is a **moving average** of "
        "the most recent readings; a Keras LSTM path exists in "
        "`lstm_forecast.py` but is inactive unless TensorFlow is installed "
        "(see the **Model** field below for the method actually used)."
    )
    sensor_map = {
        "Prishtina": "PR-001", "Prizren": "PZ-001", "Peja": "PE-001",
        "Gjakova": "GJ-001", "Mitrovica": "MI-001", "Gjilan": "GL-001",
        "Ferizaj": "FE-001", "Podujeva": "PD-001", "Vushtrri": "VU-001",
    }
    city = st.selectbox("City", ALL_CITIES)
    date = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    sid = sensor_map[city]
    hist = query_df(
        f"SELECT timestamp, temperature FROM sensor_readings "
        f"WHERE sensor_id='{sid}' AND date='{date}' LIMIT 200"
    )
    if not hist.empty:
        series = hist.sort_values("timestamp")["temperature"].dropna().tolist()
        if len(series) >= 3:
            try:
                import sys
                sys.path.insert(0, os.path.join(os.path.dirname(__file__),
                                                "..", "stream-processing"))
                from lstm_forecast import TemperatureForecaster
                f = TemperatureForecaster()
                f.fit(series)
                pred = f.predict_next(series)
                # generated-at reflects this render; it recalculates on every
                # rerun (city change / Refresh data / reload) using fresh data.
                generated_at = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

                c1, c2 = st.columns([1, 2])
                with c1:
                    st.metric(f"Predicted next temperature — {city}",
                              f"{pred:.2f} °C", help=f"forecaster mode: {f.mode}")
                with c2:
                    st.markdown(
                        f"- **Prediction generated at:** {generated_at}\n"
                        f"- **Based on latest {len(series)} readings from Cassandra** "
                        f"(sensor `{sid}`)\n"
                        f"- **Model:** {f.mode}"
                    )
                st.info(
                    "ℹ️ Because the system receives real-time IoT data, the "
                    "prediction is recalculated when new readings are refreshed — "
                    "it updates when you **change the city**, click **Refresh "
                    "data** (sidebar), or the dashboard reloads."
                )
            except Exception as exc:
                st.warning(f"Forecast unavailable: {exc}")
        else:
            st.info("Not enough readings yet to generate a forecast (need ≥ 3).")
        st.line_chart(pd.Series(series, name="temperature"))
    else:
        st.info("Not enough history to forecast yet.")


# ==========================================================================
# 7. SENSOR METADATA
# ==========================================================================
def page_metadata():
    st.title("Sensor Metadata")
    st.caption(
        "Static description of each weather station, read from the Cassandra "
        "`sensor_metadata` table (manufacturer, model, serial number, etc.)."
    )

    cols = [
        "sensor_id", "city", "manufacturer", "model", "serial_number",
        "sensor_type", "installation_date", "status", "sampling_frequency_seconds",
    ]
    meta = query_df(
        "SELECT sensor_id, city, manufacturer, model, serial_number, "
        "sensor_type, installation_date, status, sampling_frequency_seconds "
        "FROM sensor_metadata"
    )

    if meta.empty:
        st.info(
            "No sensor metadata found. Seed it with "
            "`python infrastructure/seed_metadata.py`."
        )
        return

    # Keep a stable column order and sort by sensor id
    meta = meta[[c for c in cols if c in meta.columns]].sort_values("sensor_id")

    c1, c2 = st.columns(2)
    c1.metric("Total sensors", len(meta))
    c2.metric("Active sensors", int((meta["status"] == "active").sum()))

    # Optional status filter
    statuses = sorted(meta["status"].dropna().unique().tolist())
    chosen = st.multiselect("Filter by status", statuses, default=statuses)
    if chosen:
        meta = meta[meta["status"].isin(chosen)]

    st.dataframe(meta.reset_index(drop=True), use_container_width=True)


# --------------------------------------------------------------------------
# Router
# --------------------------------------------------------------------------
PAGES = {
    "Overview": page_overview,
    "Live Readings": page_live,
    "Sensor Metadata": page_metadata,
    "Alarms": page_alarms,
    "Simulator Control": page_simulator,
    "Performance": page_performance,
    "AI / Prediction": page_ai,
}
PAGES[page]()
