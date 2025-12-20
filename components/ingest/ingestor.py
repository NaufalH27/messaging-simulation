import json
import math
import sys
from typing import Dict
import time
import uuid
import numpy as np
from obspy.clients.seedlink.easyseedlink import EasySeedLinkClient
from obspy import UTCDateTime
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
import threading
import pandas as pd

try:
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
except Exception as e:
    print(e)
    sys.exit(1)

SEEDLINK_ENDPOINT = "rtserve.iris.washington.edu:18000"

WINDOW_SECONDS = 5
WINDOW_US = WINDOW_SECONDS * 1_000_000
class SampleWindow:
    def __init__(self, fs, t0, orientations):
        self.orientation_order = tuple(orientations)
        self.fs = fs
        self.T0 = t0
        self.Tend = t0 + WINDOW_SECONDS
        self.SIZE = int(WINDOW_SECONDS * fs)
        self.uuid = uuid.uuid4()
        self.grid = {
            ch: np.full(self.SIZE, np.nan, dtype=np.float32)
            for ch in self.orientation_order
        }
        self.grid_locks = {ch: threading.Lock() for ch in self.orientation_order}
        self.channel_counts = {ch: 0 for ch in self.orientation_order}
        self.total_expected = self.SIZE * len(self.orientation_order)
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

        dt_us = round(ts.timestamp * 1_000_000 - self.T0.timestamp * 1_000_000)
        period_us = round(1_000_000 / self.fs)  
        index = round(dt_us // period_us)

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


class Sensor:
    def __init__(self, network:str, station:str, fs:int,sensor_code:str , orientation:str ):
        self.windows: Dict[int, SampleWindow] = {}
        self.windows_lock = threading.Lock()
        self.network = network
        self.station = station
        self.window_size = WINDOW_SECONDS * fs
        self.sensor_code = sensor_code # XX
        self.orientation = orientation #ZNE
        self.fs = fs
    
    def get_window(self, ts: UTCDateTime):
        ts_us = round(ts.timestamp * 1_000_000)

        with self.windows_lock:
            ref_us = round(UTCDateTime(0).timestamp * 1_000_000)

            offset_us = ts_us - ref_us
            win_index = offset_us // WINDOW_US
            window_us = ref_us + win_index * WINDOW_US

            if window_us not in self.windows:
                self.windows[window_us] = SampleWindow(
                    fs=self.fs,
                    t0=UTCDateTime(window_us / 1_000_000),
                    orientations=self.orientation
                )

            return self.windows[window_us]

    def del_windows(self, key):
        if key not in self.windows:
            return

        del self.windows[key]

    
sensors: Dict[str, Sensor] = {}
sensors_lock = threading.Lock()


class ObspyClient(EasySeedLinkClient):
    def on_data(self, trace):
        t0 = trace.stats.starttime
        tend = trace.stats.endtime
        fs = trace.stats.sampling_rate
        sensor_key = f"{trace.stats.network}.{trace.stats.station}.{trace.stats.channel[0:2]}"
        s = sensors[sensor_key]
        if s is None:
            print(f"ERROR: {sensor_key} Object not yet initialized, skipping")
            return
        if fs != s.fs:
            print(f"WARNING: {sensor_key} trace {t0} {tend} have unequal sample rate {fs} (it should be {s.fs}), skipping")
            return
            
        for i, sample in enumerate(trace.data.tolist()):
            timestamp = t0 + i / fs
            window = s.get_window(timestamp)
            window.insert_sample(timestamp, sample, trace.stats.channel[2])
            

def process_station(network, station_name, seed):
    client = ObspyClient(SEEDLINK_ENDPOINT)
    client.select_stream(network, station_name, seed)
    print(f"running client: {network} {station_name} {seed}")
    client.run()

def emitter_poll(key: str, s: Sensor):
    print(f"emitter for {key} started")

    nan_threshold = 0.95
    max_age = 60 
    last_yyy = []

    while True:
        age_now = UTCDateTime.now()
        yyy = [
            tuple(s.windows[w_key].channel_counts.values())
            for w_key in sorted(s.windows)
        ]

        if yyy != last_yyy:
            print(f"current {key} windows: {yyy} ({len(yyy)})")
        last_yyy = yyy

        with s.windows_lock:
            if not s.windows:
                time.sleep(0.1)
                continue
            keys = sorted(s.windows)
            while keys:
                w_key = keys[0]
                w = s.windows[w_key]
                with w.emit_lock:
                    if w.emitted:
                        s.del_windows(w_key)
                        keys.pop(0)
                        continue

                    filled_ratio = w.total_filled / w.total_expected
                    age = age_now - w.last_write_timestamp
                    ready_to_emit = (
                        filled_ratio >= 1.0 or
                        (filled_ratio >= nan_threshold and age > max_age)
                    )
                    ready_to_drop = (age > max_age and filled_ratio < nan_threshold)
                    if not ready_to_emit and not ready_to_drop:
                        break

                    if ready_to_emit:
                        merged = np.column_stack([w.grid[ch] for ch in w.orientation_order])

                        message = {
                            "key": key,
                            "network": s.network,
                            "station": s.station,
                            "sensor_code": s.sensor_code,
                            "sampling_rate": s.fs,
                            "orientation_order": s.orientation,
                            "starttime": w.T0.isoformat(),
                            "endtime": w.Tend.isoformat(),
                            "sample_counts": tuple(w.channel_counts.values()),
                            "trace": merged.tolist(),
                        }
                        producer.produce(
                            topic_name,
                            key=key.encode(),
                            value=json.dumps(message).encode("utf-8"),
                        )
                        producer.poll(0)
                        print(
                            f"windows {key}: sent {w_key} "
                            f"{message['starttime']}-{message['endtime']} "
                            f"{message['sample_counts']}"
                        )

                    if ready_to_drop:
                        print(
                            f"windows {key}: dropped {w_key} "
                            f"(age) {tuple(w.channel_counts.values())}"
                        )

                    w.emitted = True
                    s.del_windows(w_key)
                    keys.pop(0)

        time.sleep(0.1)



def main():
    tasks = []
    df = pd.read_csv("artifacts/station_list.csv")
    for _, row in df.iterrows():
        network = row["network"]
        station = row["station"]
        sensor_code = row["sensor_code"]          # XX
        orientation = row["orientation_order"]    # e.g. ZNE
        fs = int(row["sampling_rate"])

        if not isinstance(orientation, str) and len(orientation) != 3:
            raise ValueError(
                f"{network}.{station}.{sensor_code} invalid orientation_order: {orientation}, correct example : ZNE"
            )

        sensor_key = f"{network}.{station}.{sensor_code}"
        with sensors_lock:
            sensors[sensor_key] = Sensor(
                network=network,
                station=station,
                fs=fs,
                sensor_code=sensor_code,
                orientation=orientation
            )

        print(f"created sensor {sensor_key} ({orientation} {fs}hz)")

        for o in orientation:
            channel = sensor_code + o
            tasks.append((network, station, channel))


    threads = []
    for key, s in sensors.items():
        emitter = threading.Thread(
            target=emitter_poll,
            args=(key, s),
            daemon=True
        )
        emitter.start()
        threads.append(emitter)
    for network, station, channel in tasks:
        t = threading.Thread(
            target=process_station,
            args=(network, station, channel),
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


