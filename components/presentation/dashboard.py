import math
import streamlit as st
import numpy as np
import io
from datetime import timedelta

import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy.signal import find_peaks

from cassandra.cluster import Cluster
from minio import Minio

from queue import Queue
from threading import Lock

CASSANDRA_HOSTS = ["127.0.0.1"]
KEYSPACE = "seismic_data"
TABLE = "window_predictions"

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "seismic"

FS = 100
DET_THRESHOLD = 0.5
P_THRESHOLD = 0.3
S_THRESHOLD = 0.3
MIN_DISTANCE_SEC = 0.5

st.set_page_config(layout="wide")
st.title("Seismic Streaming Dashboard")

@st.cache_resource
def cassandra_session():
    return Cluster(CASSANDRA_HOSTS).connect(KEYSPACE)

@st.cache_resource
def minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

session = cassandra_session()
minio = minio_client()


for k, v in {
    "data": {},
    "latest_endtime": {},
    "current_key": None,
}.items():
    st.session_state.setdefault(k, v)


def make_figure(data, t_start, t_end):
    mask = (data["time"] >= t_start) & (data["time"] <= t_end)

    fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True,
        subplot_titles=("Z", "N", "E", "DET / P / S"),
    )

    colors = ["purple", "cyan", "blue"]
    for i in range(3):
        fig.add_trace(
            go.Scattergl(
                x=data["time"][mask],
                y=data["trace"][mask, i],
                connectgaps=False,
            ),
            row=i + 1,
            col=1,
        )

    fig.add_trace(go.Scattergl(x=data["time"][mask], y=data["det"][mask], name="DET"), 4, 1)
    fig.add_trace(go.Scattergl(x=data["time"][mask], y=data["p"][mask], name="P"), 4, 1)
    fig.add_trace(go.Scattergl(x=data["time"][mask], y=data["s"][mask], name="S"), 4, 1)

    dist = int(MIN_DISTANCE_SEC * data["fs"])
    p_peaks, _ = find_peaks(data["p"][mask], height=P_THRESHOLD, distance=dist)
    s_peaks, _ = find_peaks(data["s"][mask], height=S_THRESHOLD, distance=dist)

    for i in p_peaks:
        if data["det"][mask][i] >= DET_THRESHOLD:
            fig.add_vline(x=data["time"][mask][i], line_color="green", line_dash="dash")

    for i in s_peaks:
        if data["det"][mask][i] >= DET_THRESHOLD:
            fig.add_vline(x=data["time"][mask][i], line_color="red", line_dash="dash")

    fig.update_yaxes(range=[0, 1], row=4, col=1)
    fig.update_layout(height=900, hovermode="x unified")
    return fig


keys = []
rows = session.execute(f"SELECT key, starttime FROM {TABLE}")
latest = {}
for r in rows:
    latest[r.key] = max(latest.get(r.key, r.starttime), r.starttime)
keys = [k for k, _ in sorted(latest.items(), key=lambda x: x[1])]
selected_key = st.selectbox("Key", keys)

if selected_key != st.session_state.current_key:
    st.session_state.current_key = selected_key
    data = load_full_key(selected_key)
    st.session_state.data[selected_key] = data
    st.session_state.latest_endtime[selected_key] = data["time"][-1]

DATA = st.session_state.data[selected_key]

t_min, t_max = DATA["time"][0], DATA["time"][-1]

window_sec = st.slider("Window (sec)", 10, 600, 120)
center_sec = st.slider(
    "Scroll",
    0.0,
    math.ceil((t_max - t_min).total_seconds()),
    math.ceil((t_max - t_min).total_seconds()),
)

center_time = t_min + timedelta(seconds=center_sec)
half = timedelta(seconds=window_sec / 2)

chart = st.empty()
chart.plotly_chart(
    make_figure(DATA, center_time - half, center_time + half),
    width="stretch",
)


