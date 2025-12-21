import streamlit as st
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from cassandra.cluster import Cluster
from minio import Minio
import io
from scipy.signal import find_peaks
from datetime import timedelta

# =========================
# CONFIG
# =========================
CASSANDRA_HOSTS = ['127.0.0.1']
KEYSPACE = 'seismic'
TABLE = 'window_predictions'

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "seismic"

FS = 100
MAX_WINDOW_SAMPLES = 360_000
P_THRESHOLD = 0.5
S_THRESHOLD = 0.5
MIN_DISTANCE_SEC = 0.5

# =========================
# STREAMLIT UI
# =========================
st.set_page_config(layout="wide")
st.title("Seismic Stream Viewer")

# =========================
# CASSANDRA CONNECTION
# =========================
@st.cache_resource
def get_session():
    cluster = Cluster(CASSANDRA_HOSTS)
    return cluster.connect(KEYSPACE)

session = get_session()

# =========================
# LOAD AVAILABLE KEYS / MODELS
# =========================
@st.cache_data
def load_keys_models():
    rows = session.execute(
        f"SELECT DISTINCT key, model_name FROM {TABLE}"
    )
    return sorted({(r.key, r.model_name) for r in rows})

pairs = load_keys_models()

key = st.selectbox("Station Key", sorted(set(p[0] for p in pairs)))
models = sorted(p[1] for p in pairs if p[0] == key)
model = st.selectbox("Model", models)

# =========================
# LOAD DATA
# =========================
@st.cache_data(show_spinner=True)
def load_data(key, model):
    rows = session.execute(
        f"""
        SELECT * FROM {TABLE}
        WHERE key = %s AND model_name = %s
        ORDER BY starttime ASC
        """,
        (key, model)
    )

    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    traces, dets, ps, ss = [], [], [], []
    t0 = None

    for row in rows:
        if t0 is None:
            t0 = row.starttime

        response = minio_client.get_object(MINIO_BUCKET, row.minio_ref)
        data_bytes = response.read()
        response.close()
        response.release_conn()

        with np.load(io.BytesIO(data_bytes)) as f:
            traces.append(f["trace"])
            dets.append(f["dd"])
            ps.append(f["pp"])
            ss.append(f["ss"])

    return (
        np.concatenate(traces),
        np.concatenate(dets),
        np.concatenate(ps),
        np.concatenate(ss),
        t0,
    )

merged_trace, merged_det, merged_p, merged_s, t0 = load_data(key, model)

# =========================
# METADATA
# =========================
total_samples = len(merged_det)
end_datetime = t0 + timedelta(seconds=total_samples / FS)

st.info(
    f"""
**Total samples:** {total_samples:,}  
**Start time:** {t0}  
**End time:** {end_datetime}
"""
)

# =========================
# SAMPLE SELECTION
# =========================
start_sample = st.number_input(
    "Start sample",
    min_value=0,
    max_value=total_samples - 1,
    value=0,
    step=1000,
)

end_sample = st.number_input(
    "End sample",
    min_value=1,
    max_value=total_samples,
    value=min(6000, total_samples),
    step=1000,
)

# Enforce max window
if end_sample - start_sample > MAX_WINDOW_SAMPLES:
    start_sample = max(0, end_sample - MAX_WINDOW_SAMPLES)
    st.warning(
        f"Window limited to {MAX_WINDOW_SAMPLES:,} samples. "
        f"Start adjusted to {start_sample}."
    )

# =========================
# WINDOW
# =========================
trace_win = merged_trace[start_sample:end_sample]
det_win = merged_det[start_sample:end_sample]
p_win = merged_p[start_sample:end_sample]
s_win = merged_s[start_sample:end_sample]

dt = 1 / FS
time_axis = [
    t0 + timedelta(seconds=(start_sample + i) * dt)
    for i in range(len(det_win))
]

# =========================
# PEAK PICKING
# =========================
MIN_DISTANCE = int(MIN_DISTANCE_SEC * FS)
p_peaks, _ = find_peaks(p_win, height=P_THRESHOLD, distance=MIN_DISTANCE)
s_peaks, _ = find_peaks(s_win, height=S_THRESHOLD, distance=MIN_DISTANCE)

# =========================
# PLOTLY FIGURE
# =========================
fig = make_subplots(
    rows=4,
    cols=1,
    shared_xaxes=True,
    vertical_spacing=0.02,
    subplot_titles=("Channel Z", "Channel N", "Channel E", "Probabilities"),
)

for i, ch in enumerate(["Z", "N", "E"]):
    fig.add_trace(
        go.Scatter(x=time_axis, y=trace_win[:, i], mode="lines", name=f"{ch}"),
        row=i + 1,
        col=1,
    )

    for p in p_peaks:
        fig.add_vline(x=time_axis[p], line_color="green", line_dash="dash")

    for s in s_peaks:
        fig.add_vline(x=time_axis[s], line_color="red", line_dash="dash")

fig.add_trace(go.Scatter(x=time_axis, y=det_win, name="Detection"), row=4, col=1)
fig.add_trace(go.Scatter(x=time_axis, y=p_win, name="P prob"), row=4, col=1)
fig.add_trace(go.Scatter(x=time_axis, y=s_win, name="S prob"), row=4, col=1)

fig.update_yaxes(range=[0, 1], row=4, col=1)
fig.update_layout(height=900, hovermode="x unified")

st.plotly_chart(fig, use_container_width=True)
