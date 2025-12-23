from datetime import datetime, time, timedelta, timezone
import pandas as pd
import streamlit as st
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from cassandra.cluster import Cluster
import io
import time as t
from scipy.signal import find_peaks
from datetime import timedelta
import clickhouse_connect

FS = 100
DET_TRESHOLD = 0.8
P_THRESHOLD = 0.5
S_THRESHOLD = 0.5
MIN_DISTANCE_SEC = 0.5

# Cassandra
CASSANDRA_HOSTS = ['127.0.0.1']
KEYSPACE = 'seismic'
TABLE = 'predicted_samples'


@st.cache_resource
def cassandra_cluster():
    return Cluster(CASSANDRA_HOSTS)

# ClickHouse
CH_HOST = "localhost"
CH_PORT = 8123
CH_USER = "admin"
CH_PASS = "admin"
CH_TABLE = "predicted_samples"

st.title("Seismic")
st.set_page_config(layout="wide")

tab = st.radio(
    "View",
    ["Live Stream", "Archive"],
    horizontal=True,
    key="active_tab"
)

def plot_waveform_plotly(
    trace_win,
    det_win,
    p_win,
    s_win,
    uirevision_key,
    time_axis,
    P_THRESHOLD=0.5,
    S_THRESHOLD=0.5,
):
    time_axis = pd.to_datetime(time_axis, utc=True)
    time_axis = time_axis.tz_convert("Etc/GMT-7")  
    p_peaks, _ = find_peaks(np.nan_to_num(p_win), height=P_THRESHOLD)
    s_peaks, _ = find_peaks(np.nan_to_num(s_win), height=S_THRESHOLD)

    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        subplot_titles=[
            "Channel Z",
            "Channel N",
            "Channel E",
            "Probabilities",
        ],
    )

    channel_titles = ["Z", "N", "E"]
    for i in range(3):
        fig.add_trace(
            go.Scatter(
                x=time_axis,
                y=trace_win[:, i],
                mode="lines",
                name=f"Channel {channel_titles[i]}",
                line=dict(width=1),
            ),
            row=i + 1,
            col=1,
        )

        # P picks
        for idx in p_peaks:
            fig.add_vline(
                x=time_axis[idx],
                line_color="red",
                line_dash="dash",
                line_width=1,
                row=i + 1,
                col=1,
            )

        # S picks
        for idx in s_peaks:
            fig.add_vline(
                x=time_axis[idx],
                line_color="purple",
                line_dash="dash",
                line_width=1,
                row=i + 1,
                col=1,
            )

    fig.add_trace(
        go.Scatter(x=time_axis, y=det_win, mode="lines", name="Detection"),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(x=time_axis, y=p_win, mode="lines", name="P probability"),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(x=time_axis, y=s_win, mode="lines", name="S probability"),
        row=4,
        col=1,
    )

    fig.update_yaxes(
        range=[0, 1],
        title_text="Probability",
        row=4,
        col=1,
    )
    fig.update_xaxes(title_text="Time", row=4, col=1)

    fig.update_layout(
        uirevision=uirevision_key,
        height=900,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=40, r=40, t=60, b=40),
    )
    
    return fig


def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASS,
    )


if tab == "Live Stream":
    st.header("Live Stream (ClickHouse)")

    LIVE_REFRESH_SEC = 1
    @st.cache_data(ttl=30)
    def load_key_model_pairs():
        ch = get_ch()
        result = ch.query(f"""
            SELECT key, model_name
            FROM {CH_TABLE}
            GROUP BY key, model_name
            ORDER BY key, model_name
        """)
        return result.result_rows


    FS = 100
    FREQ = "10ms"
    WINDOW_SECONDS = 60
    DECIMATE = 1


    rows = load_key_model_pairs()
    pair_labels = {
        f"{key} | {model}": (key, model) for key, model in rows
    }

    selected_label = st.selectbox(
        "Station | Model",
        sorted(pair_labels.keys()),
        key="station_model_select"
    )
    selected_key, selected_model = pair_labels[selected_label]

    plot_live = st.empty()

    query = """
        WITH
            (
                SELECT max(ts)
                FROM predicted_samples
                WHERE key = {key:String}
                AND model_name = {model:String}
            ) AS latest_ts

        SELECT
            ts, orientation_order, trace1, trace2, trace3, dd, pp, ss
        FROM predicted_samples FINAL
        WHERE key = {key:String}
        AND model_name = {model:String}
        AND ts >= latest_ts - INTERVAL 60 SECOND
        ORDER BY ts
    """

    @st.fragment(run_every="2s")
    def live_refresh():
        df = get_ch().query_df(
            query,
            parameters={
                "key": selected_key,
                "model": selected_model,
            },
        )

        if not df.empty:
            df["ts"] = pd.to_datetime(df["ts"], utc=True)
            df = df.set_index("ts").sort_index()

            end_ts = df.index.max()
            start_ts = end_ts - pd.Timedelta(seconds=WINDOW_SECONDS)

            full_index = pd.date_range(start_ts, end_ts, freq=FREQ)
            df_win = pd.DataFrame(
                index=full_index,
                columns=["trace1", "trace2", "trace3", "dd", "pp", "ss"]
            )

            df = df.loc[start_ts:end_ts]
            df_win.update(df)
        else:
            end_ts = pd.Timestamp.utcnow()
            start_ts = end_ts - pd.Timedelta(seconds=WINDOW_SECONDS)
            full_index = pd.date_range(start_ts, end_ts, freq=FREQ)
            df_win = pd.DataFrame(
                index=full_index,
                columns=["trace1", "trace2", "trace3", "dd", "pp", "ss"]
            )

        trace_np = df_win[["trace1", "trace2", "trace3"]].to_numpy()
        det_np = df_win["dd"].to_numpy()
        p_np = df_win["pp"].to_numpy()
        s_np = df_win["ss"].to_numpy()

        time_axis = df_win.index if DECIMATE == 1 else df_win.index[::DECIMATE]

        if len(trace_np) > 1:
            fig = plot_waveform_plotly(
                trace_np,
                det_np,
                p_np,
                s_np,
                time_axis=time_axis,
                P_THRESHOLD=P_THRESHOLD,
                S_THRESHOLD=S_THRESHOLD,
                uirevision_key=f"live-{selected_key}-{selected_model}"
            )

            plot_live.plotly_chart(
                fig,
                use_container_width=True,
                key="live_plot"
            )
            lateness = datetime.now(timezone.utc) - end_ts

            st.markdown(
                f"<div style='text-align: right;'>lateness: {lateness}</div>",
                unsafe_allow_html=True
            )
    live_refresh()

if tab == "Archive":
    st.header("Archive Data (Scylla)")
    
    @st.cache_resource
    def get_session():
        return cassandra_cluster().connect(KEYSPACE)

    session = get_session()

    rows = session.execute(
        f"SELECT DISTINCT key, model_name FROM predicted_sample_bucket_by_day"
    )
    pairs = sorted({(r.key, r.model_name) for r in rows})
    pair_labels = {
        f"{k} | {m}": (k, m) for k, m in pairs
    }

    # Track when selection changes to reset times
    def on_selection_change():
        """Clear time state when selection changes"""
        for key in ["start_h", "start_m", "start_s", "end_h", "end_m", "end_s"]:
            if key in st.session_state:
                del st.session_state[key]

    selected_label = st.selectbox(
        "Station | Model",
        sorted(pair_labels.keys()),
        key="archive_station_model",
        on_change=on_selection_change
    )
    selected_key, selected_model = pair_labels[selected_label]

    day_rows = session.execute(
        """
        SELECT hour
        FROM predicted_sample_bucket_by_day
        WHERE key = %s AND model_name = %s
        """,
        (selected_key, selected_model),
    )
    available_days = [r.hour for r in day_rows]
    selected_day = st.selectbox(
        "Available Day",
        sorted(available_days),
        key="archive_day",
        on_change=on_selection_change
    )

    @st.cache_data(show_spinner=False)
    def load_historical(key, model, day):
        # day == hour bucket

        if day.tzinfo is None:
            day = day.replace(tzinfo=timezone.utc)

        day = (
            day.astimezone(timezone.utc)
            .replace(minute=0, second=0, microsecond=0)
        )

        session = get_session()
        rows = session.execute(
            """
            SELECT ts, orientation_order, trace1, trace2, trace3, dd, pp, ss
            FROM predicted_samples
            WHERE key = %s AND model_name = %s AND hour = %s
            ORDER BY ts ASC
            """,
            (key, model, day),
        )
        rows = list(rows)
        best = {}
        for r in rows:
            ts = r.ts
            if ts not in best or r.dd > best[ts].dd:
                best[ts] = r

        traces, dets, ps, ss, times = [], [], [], [], []
        for r in best.values():
            traces.append([r.trace1, r.trace2, r.trace3])
            dets.append(r.dd)
            ps.append(r.pp)
            ss.append(r.ss)
            times.append(r.ts)

        return (
            np.array(traces),
            np.array(dets),
            np.array(ps),
            np.array(ss),
            np.array(times),
        )

    trace, det, p, s, time_axis = load_historical(selected_key, selected_model, selected_day)
    if trace is None or trace.size == 0:
        st.warning("No Data")
        st.stop()


    df = pd.DataFrame(
        {
            "trace1": trace[:, 0],
            "trace2": trace[:, 1],
            "trace3": trace[:, 2],
            "det": det,
            "p": p,
            "s": s,
        },
        index=pd.to_datetime(time_axis, utc=True) 
    )


    min_ts = pd.to_datetime(time_axis[0])
    max_ts = pd.to_datetime(time_axis[-1])
    min_time = min_ts.time()
    max_time = max_ts.time()

    # Initialize times to full range if not set
    def init_time(prefix, t):
        reset = st.session_state.pop("_reset_time", False)

        for k, v in zip(("h", "m", "s"), (t.hour, t.minute, t.second)):
            state_key = f"{prefix}_{k}"
            if reset or state_key not in st.session_state:
                st.session_state[state_key] = v


    init_time("start", min_time)
    init_time("end", max_time)

    min_abs_sec = min_ts.hour * 3600 + min_ts.minute * 60 + min_ts.second
    max_abs_sec = max_ts.hour * 3600 + max_ts.minute * 60 + max_ts.second

    def get_time(prefix):
        return time(
            st.session_state[f"{prefix}_h"],
            st.session_state[f"{prefix}_m"],
            st.session_state[f"{prefix}_s"],
        )

    def set_time(prefix, t):
        st.session_state[f"{prefix}_h"] = t.hour
        st.session_state[f"{prefix}_m"] = t.minute
        st.session_state[f"{prefix}_s"] = t.second

    def hms_to_abs_seconds(h, m, s):
        return h * 3600 + m * 60 + s

    def abs_seconds_to_hms(sec):
        h = sec // 3600
        sec %= 3600
        m = sec // 60
        s = sec % 60
        return h, m, s

    def validate_times():
        if "start_h" not in st.session_state or "end_h" not in st.session_state:
            return

        # START
        s_sec = hms_to_abs_seconds(
            st.session_state.start_h,
            st.session_state.start_m,
            st.session_state.start_s,
        )
        # END
        e_sec = hms_to_abs_seconds(
            st.session_state.end_h,
            st.session_state.end_m,
            st.session_state.end_s,
        )

        # Clip to absolute bounds
        s_sec = max(min_abs_sec, min(s_sec, max_abs_sec))
        e_sec = max(min_abs_sec, min(e_sec, max_abs_sec))

        # Enforce end > start
        if e_sec <= s_sec:
            if s_sec >= max_abs_sec:
                s_sec = max(max_abs_sec - 1, min_abs_sec)
                e_sec = max_abs_sec
            else:
                e_sec = s_sec + 1

        # Write back
        st.session_state.start_h, st.session_state.start_m, st.session_state.start_s = abs_seconds_to_hms(s_sec)
        st.session_state.end_h, st.session_state.end_m, st.session_state.end_s = abs_seconds_to_hms(e_sec)

    def reset_everything():
        st.session_state["_reset_time"] = True
        load_historical.clear()

    col1, spacer, col2 = st.columns([1, 0.2, 1])

    with col1:
        st.markdown(f"**Start time** \nMin: `{min_ts.strftime('%H:%M:%S')}`")
        h, m, s = st.columns(3)
        h.number_input("HH", 0, 23, key="start_h", on_change=validate_times, label_visibility="collapsed")
        m.number_input("MM", -1, 60, key="start_m", on_change=validate_times, label_visibility="collapsed")
        s.number_input("SS", -1, 60, key="start_s", on_change=validate_times, label_visibility="collapsed")

    with col2:
        st.markdown(f"**End time** \nMax: `{max_ts.strftime('%H:%M:%S')}`")
        h, m, s = st.columns(3)
        h.number_input("HH", 0, 23, key="end_h", on_change=validate_times, label_visibility="collapsed")
        m.number_input("MM", -1, 60, key="end_m", on_change=validate_times, label_visibility="collapsed")
        s.number_input("SS", -1, 60, key="end_s", on_change=validate_times, label_visibility="collapsed")

    if st.button(
        "Reset & Refetch",
        key="reset_archive",
        help="Reset time range and refetch data from database",
    ):
        reset_everything()
        st.rerun()


    st.divider()
    st.write("Start:", get_time("start"))
    st.write("End:", get_time("end"))

    freq = "10ms"  # 100 Hz
    full_index = pd.date_range(
        start=df.index.min(),
        end=df.index.max(),
        freq="10ms",
    )
    df = df.reindex(full_index)

    base = df.index.min()  # this is 2025-12-23 04:06:15+00

    start_dt = base.replace(
        hour=get_time("start").hour,
        minute=get_time("start").minute,
        second=get_time("start").second,
    )

    end_dt = base.replace(
        hour=get_time("end").hour,
        minute=get_time("end").minute,
        second=get_time("end").second,
    )

    df_win = df.loc[start_dt:end_dt]

    trace_np = df_win[["trace1", "trace2", "trace3"]].to_numpy()
    det_np = df_win["det"].to_numpy()
    p_np = df_win["p"].to_numpy()
    s_np = df_win["s"].to_numpy()

    if len(df_win) > 1:
        fig = plot_waveform_plotly(
            trace_np,
            det_np,
            p_np,
            s_np,
            time_axis=df_win.index,
            P_THRESHOLD=P_THRESHOLD,
            S_THRESHOLD=S_THRESHOLD,
            uirevision_key=f"archive-{selected_key}-{selected_model}-{selected_day}"
        )
        st.plotly_chart(
            fig,
            use_container_width=True,
            key="archive_plot"
        )