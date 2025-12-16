import json
from typing import Dict
import time
import uuid
import numpy as np
from obspy.clients.seedlink.easyseedlink import EasySeedLinkClient
from obspy import UTCDateTime
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
import threading

admin_client = AdminClient({
    "bootstrap.servers": "localhost:9092"
})

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

topic_name = "seismic"
new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
fs = admin_client.create_topics([new_topic])

for topic, f in fs.items():
    try:
        f.result()
        print(f"Topic {topic} created")
    except Exception:
        print(f"INFO: Topic '{topic}' already exists")

SEEDLINK_ENDPOINT = "rtserve.iris.washington.edu:18000"

WINDOW_SECONDS = 4
class SampleWindow:
    CHANNEL_ORDER = ("Z", "N", "E")

    def __init__(self, fs, t0):
        self.fs = fs
        self.T0 = t0
        self.Tend = t0 + WINDOW_SECONDS
        self.SIZE = int(WINDOW_SECONDS * fs)
        self.uuid = uuid.uuid4()
        self.grid = {
            ch: np.full(self.SIZE, np.nan, dtype=np.float32)
            for ch in self.CHANNEL_ORDER
        }
        self.grid_locks = {ch: threading.Lock() for ch in self.CHANNEL_ORDER}
        self.channel_counts = {ch: 0 for ch in self.CHANNEL_ORDER}
        self.total_expected = self.SIZE * len(self.CHANNEL_ORDER)
        self.total_filled = 0
        self.emitted = False
        self.emit_lock = threading.Lock()
        self.last_write_timestamp = UTCDateTime.now()

    def insert_sample(self, ts: UTCDateTime, sample, channel: str):
        with self.emit_lock:
            if self.emitted:
                return


        if channel not in self.grid:
            raise ValueError(f"Invalid channel: {channel}")

        dt = ts - self.T0
        index = round(dt * self.fs)

        if not (0 <= index < self.SIZE):
            raise ValueError(
                f"Invalid timestamp ({self.T0}-{self.Tend}): {ts}"
            )

        with self.grid_locks[channel]:
            if np.isnan(self.grid[channel][index]):
                self.grid[channel][index] = sample
                self.channel_counts[channel] += 1
                self.total_filled += 1
            else:
                self.grid[channel][index] = sample
        
        self.last_write_timestamp = UTCDateTime.now()


class Station:
    def __init__(self, network:str, station:str):
        self.windows: Dict[int, SampleWindow] = {}
        self.windows_lock = threading.Lock()
        self.network = network
        self.station = station
    
    def get_window(self, ts: UTCDateTime, fs):
        with self.windows_lock:
            ref_t0 = UTCDateTime(0)

            offset = ts - ref_t0
            win_index = float(offset // WINDOW_SECONDS)
            window_t0 = ref_t0 + win_index * WINDOW_SECONDS
            window_key = float(window_t0)

            if window_key not in self.windows:
                self.windows[window_key] = SampleWindow(
                    fs=fs,
                    t0=window_t0,
                )

            return self.windows[window_key]

    def del_windows(self, key):
        if key not in self.windows:
            return

        del self.windows[key]

    
stations: Dict[str, Station] = {}
stations_lock = threading.Lock()

def get_station(network, station) -> Station:
    with stations_lock:  
        key = f"{network}.{station}"
        if key not in stations:
            now = UTCDateTime.now()
            now = UTCDateTime(
                year=now.year, month=now.month, day=now.day,
                hour=now.hour, minute=now.minute, second=now.second
            )
            stations[key] = Station(network, station)
            print(f"created {key}")
        return stations[key]

class ObspyClient(EasySeedLinkClient):
    def on_data(self, trace):
        t0 = trace.stats.starttime
        fs = trace.stats.sampling_rate
        s = get_station(trace.stats.network, trace.stats.station)
        for i, sample in enumerate(trace.data.tolist()):
            timestamp = t0 + i / fs
            window = s.get_window(timestamp, fs)
            window.insert_sample(timestamp, sample, trace.stats.channel[2])
            

def process_station(network, station_name, seed):
    client = ObspyClient(SEEDLINK_ENDPOINT)
    client.select_stream(network, station_name, seed)
    print(f"running client: {network} {station_name} {seed}")
    client.run()

def emitter_poll(network, station):
    s = get_station(network, station)
    print("emitter started")
    a = [None]

    while True:
        ws = s.windows.copy()
        ws = dict(sorted(ws.items()))
        emit_jobs = []
        drop_keys = []
        age_now = UTCDateTime.now()
        with s.windows_lock:
            yyy = []
            for w in ws.values():
                yyy.append(tuple(w.channel_counts.values()))
            if a != yyy:
                print(f"current windows : {yyy} {len(yyy)}")
            a = yyy
            for key in list(s.windows.keys()):
                w = s.windows[key]
                with w.emit_lock:
                    if w.emitted:
                        continue

                    filled_ratio = w.total_filled / w.total_expected
                    age = age_now - w.last_write_timestamp
                    nan_treshold = 0.95
                    max_age = 60

                    if filled_ratio >= 1.0 or (filled_ratio >= nan_treshold and age > max_age):
                        merged = np.column_stack([
                            w.grid[ch] for ch in w.CHANNEL_ORDER
                        ])

                        message = {
                            "network": network,
                            "station": station,
                            "starttime": w.T0.isoformat(),
                            "endtime": w.Tend.isoformat(),
                            "sampling_rate": w.fs,
                            "samples_per_channel": w.SIZE,
                            "num_channels": len(w.CHANNEL_ORDER),
                            "channel_order" : w.CHANNEL_ORDER,
                            "total_samples": w.SIZE * len(w.CHANNEL_ORDER),
                            "data": merged.tolist(),
                            "sample_counts" : tuple(w.channel_counts.values())
                        }

                        w.emitted = True
                        emit_jobs.append((key, message))

                    elif age > max_age and filled_ratio < nan_treshold:
                        w.emitted = True
                        drop_keys.append(key)

            for key, message in emit_jobs:
                producer.produce(topic_name, value=json.dumps(message).encode("utf-8"))
                producer.poll(0)
                print(f"send {key}: {message['starttime']}-{message['endtime']} ({message['sample_counts']})")

            for key in drop_keys:
                print(f"{key} deleted (age) {tuple(s.windows[key].channel_counts.values())}")
                s.del_windows(key)

            for key, _ in emit_jobs:
                print(f"{key} deleted (emitted to kafka)")
                s.del_windows(key)
        time.sleep(0.1)


def main():
    tasks = []
    # df = pd.read_csv("station_list.csv")
    # for _, row in df.iterrows():
    #     if row["station"] == "GENI":
    #         network = row["network"]
    #         station_name = row["station"]
    #         seed = row["channel"]
    #         tasks.append((network, station_name, seed))

    tasks.append(("XL", "MG08", "HHN"))
    tasks.append(("XL", "MG08", "HHE"))
    tasks.append(("XL", "MG08", "HHZ"))

    threads = []
    emitter = threading.Thread(
        target=emitter_poll,
        args=("XL", "MG08"),
        daemon=True
    )
    emitter.start()
    threads.append(emitter)
    for network, station_name, seed in tasks:
        t = threading.Thread(
            target=process_station,
            args=(network, station_name, seed),
            daemon=True
        )
        t.start()
        threads.append(t)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt, stopping…")
    finally:
        print("Terminating processes…")
        producer.flush()


if __name__ == "__main__":
    main()


