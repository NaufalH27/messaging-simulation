import json
import logging
from typing import Dict
import time
import uuid
import numpy as np
from obspy import UTCDateTime
import threading
from source.infra import KafkaProducerService 

FACTOR = 1_000_000
logger = logging.getLogger("ingest")

class SampleWindow:
    def __init__(self, fs, t0, orientations, window_seconds):
        self.WINDOW_SECONDS = window_seconds
        self.orientation_order = tuple(orientations)
        self.fs = fs
        self.T0 = t0
        self.Tend = t0 + self.WINDOW_SECONDS
        self.SIZE = int(self.WINDOW_SECONDS * fs)
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

        dt_us = round(ts.timestamp * FACTOR - self.T0.timestamp * FACTOR)
        period_us = round(FACTOR / self.fs)  
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

class IngestSensor:
    def __init__(self, key:str, network:str, station:str, fs:int,sensor_code:str , orientation:str, window_seconds:int ):
        self.key = key
        self.network = network
        self.station = station
        self.sensor_code = sensor_code # XX
        self.orientation = orientation #ZNE
        self.fs = fs
        self.WINDOW_SECONDS = window_seconds
        self.WINDOW_US = self.WINDOW_SECONDS * FACTOR
        self.windows: Dict[int, SampleWindow] = {}
        self.windows_lock = threading.Lock()
        self.window_size = self.WINDOW_SECONDS * fs
    
    def get_window(self, ts: UTCDateTime):
        ts_us = round(ts.timestamp * FACTOR)

        with self.windows_lock:
            ref_us = round(UTCDateTime(0).timestamp * FACTOR)

            offset_us = ts_us - ref_us
            win_index = offset_us // self.WINDOW_US
            window_us = ref_us + win_index * self.WINDOW_US

            if window_us not in self.windows:
                self.windows[window_us] = SampleWindow(
                    fs=self.fs,
                    t0=UTCDateTime(window_us / FACTOR),
                    orientations=self.orientation,
                    window_seconds=self.WINDOW_SECONDS
                )

            return self.windows[window_us]

    def del_windows(self, key):
        if key not in self.windows:
            return

        del self.windows[key]

def process_raw_trace(trace, sensor: IngestSensor):
    t0 = trace.stats.starttime
    tend = trace.stats.endtime
    fs = trace.stats.sampling_rate
    sensor_key = f"{trace.stats.network}.{trace.stats.station}.{trace.stats.channel[0:2]}"
    if sensor. key != sensor_key:
        logger.warning(f"{sensor_key} shouldnt be in {sensor.key} thread, skipping")
        return
        
    if fs != sensor.fs:
        logger.warning(f"{sensor_key} trace {t0} {tend} have unequal sample rate {fs} (it should be {sensor.fs}), skipping")
        return
        
    for i, sample in enumerate(trace.data.tolist()):
        timestamp = t0 + i / fs
        window = sensor.get_window(timestamp)
        window.insert_sample(timestamp, sample, trace.stats.channel[2])


def window_emitter(sensor:IngestSensor, producerService :KafkaProducerService, topic_name:str):
    logger.info(f"emitter for {sensor.key} started")
    producer = producerService.get_producer()

    nan_threshold = 0.95
    max_age = 60 
    last_yyy = []

    while True:
        age_now = UTCDateTime.now()
        yyy = [
            tuple(sensor.windows[w_key].channel_counts.values())
            for w_key in sorted(sensor.windows)
        ]

        if yyy != last_yyy:
            logger.debug(f"current {sensor.key} windows: {yyy} ({len(yyy)})")
        last_yyy = yyy

        with sensor.windows_lock:
            if not sensor.windows:
                time.sleep(0.1)
                continue
            keys = sorted(sensor.windows)
            while keys:
                w_key = keys[0]
                w = sensor.windows[w_key]
                with w.emit_lock:
                    if w.emitted:
                        sensor.del_windows(w_key)
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
                            "key": sensor.key,
                            "network": sensor.network,
                            "station": sensor.station,
                            "sensor_code": sensor.sensor_code,
                            "sampling_rate": sensor.fs,
                            "orientation_order": sensor.orientation,
                            "starttime": w.T0.isoformat(),
                            "endtime": w.Tend.isoformat(),
                            "sample_counts": tuple(w.channel_counts.values()),
                            "trace": merged.tolist(),
                        }
                        producer.produce(
                            topic_name,
                            key=sensor.key.encode(),
                            value=json.dumps(message).encode("utf-8"),
                        )
                        producer.poll(0)
                        logger.debug(
                            f"windows {sensor.key}: sent {w_key} "
                            f"{message['starttime']}-{message['endtime']} "
                            f"{message['sample_counts']}"
                        )

                    if ready_to_drop:
                        logger.debug(
                            f"windows {sensor.key}: dropped {w_key} "
                            f"(age) {tuple(w.channel_counts.values())}"
                        )

                    w.emitted = True
                    sensor.del_windows(w_key)
                    keys.pop(0)

        time.sleep(0.1)
