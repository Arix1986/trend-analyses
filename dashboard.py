"""
Dashboard Streamlit - Trend Detector
Lee output/trend_data.json y muestra métricas en tiempo real.
"""
import streamlit as st
import pandas as pd
import json
import os
import time
from datetime import datetime

st.set_page_config(
    page_title="Trend Detector",
    page_icon="📊",
    layout="wide",
)

OUTPUT_FILE = "output/trend_data.json"
REFRESH_INTERVAL = 3  # segundos


def load_data():
    if not os.path.exists(OUTPUT_FILE):
        return pd.DataFrame()
    try:
        with open(OUTPUT_FILE, "r") as f:
            data = json.load(f)
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df["window_end"] = pd.to_datetime(df["window_end"])
        return df
    except (json.JSONDecodeError, FileNotFoundError):
        return pd.DataFrame()


# ---------- UI ----------
st.title("📊 Social Trend Detector")
st.caption("Apache Beam + Kafka | Actualización en tiempo real")

# Placeholder para auto-refresh
placeholder = st.empty()

# Auto-refresh loop
while True:
    df = load_data()

    with placeholder.container():
        if df.empty:
            st.warning("⏳ Esperando datos... Asegúrate de que el producer y pipeline están corriendo.")
            st.code(
                "# Terminal 1: docker compose up -d\n"
                "# Terminal 2: python producer.py\n"
                "# Terminal 3: python pipeline.py\n"
                "# Terminal 4: streamlit run dashboard.py",
                language="bash",
            )
        else:
            # --- Métricas principales ---
            col1, col2, col3, col4 = st.columns(4)

            total_events = df["count"].sum()
            unique_topics = df["topic"].nunique()
            latest_window = df["window_end"].max().strftime("%H:%M:%S")
            top_topic = df.groupby("topic")["count"].sum().idxmax()

            col1.metric("Total eventos procesados", f"{total_events:,}")
            col2.metric("Topics únicos", unique_topics)
            col3.metric("Última ventana", latest_window)
            col4.metric("Top trend", f"#{top_topic}")

            st.divider()

            # --- Gráficos ---
            left, right = st.columns(2)

            with left:
                st.subheader("Conteo acumulado por topic")
                topic_totals = (
                    df.groupby("topic")["count"]
                    .sum()
                    .sort_values(ascending=True)
                    .reset_index()
                )
                st.bar_chart(topic_totals, x="topic", y="count", horizontal=True)

            with right:
                st.subheader("Evolución temporal")
                # Pivot para line chart
                pivot = df.pivot_table(
                    index="window_end",
                    columns="topic",
                    values="count",
                    aggfunc="sum",
                    fill_value=0,
                )
                st.line_chart(pivot)

            # --- Tabla de datos recientes ---
            st.subheader("Últimas ventanas procesadas")
            recent = df.sort_values("window_end", ascending=False).head(20)
            st.dataframe(
                recent,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "topic": st.column_config.TextColumn("Topic", width="medium"),
                    "count": st.column_config.NumberColumn("Eventos", width="small"),
                    "window_end": st.column_config.DatetimeColumn(
                        "Ventana", format="HH:mm:ss"
                    ),
                },
            )

    time.sleep(REFRESH_INTERVAL)
    st.rerun()
