from datetime import datetime, timedelta
import json
import logging
import math
import threading
import time
from typing import Dict, List, Tuple
import numpy as np
from obspy import UTCDateTime
from pydantic import BaseModel, ConfigDict, field_validator
from scipy.signal import resample_poly
from source.infra import KafkaConsumerService, KafkaProducerService, EqTransformerService
from source.lib.eqt_tf29.core import predict

logger = logging.getLogger("inference")

class SeismicWindow(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    key:str
    network: str
    station: str
    sensor_code:str
    sampling_rate: float
    orientation_order: str
    starttime: datetime
    endtime: datetime
    sample_counts: Tuple[int, ...]
    trace: np.ndarray

    @field_validator('trace', mode='before')
    @classmethod
    def convert_data_to_numpy(cls, v):
        return np.array(v, dtype=np.float32)

class InferenceSensor:
    def __init__(
        self,
        key: str,
        network: str,
        station: str,
        fs: int,
        sensor_code: str,
        orientation: str,
        window_seconds: int
    ):
        self.key = key
        self._windows: Dict[int, SeismicWindow] = {}
        self._windows_lock = threading.Lock()
        self.network = network
        self.station = station
        self.sensor_code = sensor_code
        self.orientation = orientation
        self.fs = fs
        self.window_seconds = window_seconds
        self.window_size = window_seconds * fs

    def insert_window(self, window: SeismicWindow) -> None:
        delta_seconds = (window.starttime - UTCDateTime(0).datetime).total_seconds()
        if delta_seconds % self.window_seconds != 0:
            raise ValueError("unaligned window start")

        delta = int(delta_seconds // self.window_seconds)
        with self._windows_lock:
            self._windows[delta] = window


    def get_sorted_windows(self) -> List[SeismicWindow | None]:
        with self._windows_lock:
            windows = dict(self._windows)
        if len(windows) == 0:
            return []

        sorted_items = sorted(windows.items())
        keys = [k for k, _ in sorted_items]

        beginning = keys[0]
        end = keys[-1]
        size = end - beginning + 1

        windows_arr: List[tuple[SeismicWindow] | None] = [None] * size

        for k, w in sorted_items:
            windows_arr[k - beginning] = w

        return windows_arr

    def get_window_batches(
        self, overlap: int
    ) -> List[List[SeismicWindow]]:

        context = 60 // self.window_seconds
        if overlap < 0 or overlap >= context:
            raise ValueError("overlap must be >= 0 and < context")
        with self._windows_lock:
            windows = dict(self._windows)
        if len(windows) < context:
            return []

        sorted_items = sorted(windows.items())
        keys = [k for k, _ in sorted_items]
        beginning = keys[0]
        end = keys[-1]
        size = end - beginning + 1
        windows_arr: List[tuple[int, SeismicWindow] | None] = [None] * size
        for k, w in sorted_items:
            windows_arr[k - beginning] = (k, w)

        n = len(windows_arr)
        valid_starts : List[int] = []
        for i in range(0, n - context + 1):
            if any(v is None for v in windows_arr[i:i+context]):
                continue
            valid_starts.append(i)

        if not valid_starts:
            return []

        strided_starts = []
        stride = context - overlap
        i = 0
        while i < len(valid_starts):
            st = valid_starts[i]
            strided_starts.append(st)
            nextst = st + stride
            if nextst in valid_starts:
                i = valid_starts.index(nextst)
            else:
                i += 1

        results: List[List[SeismicWindow]] = []
        keys_to_delete: set[int] = set()
        for start in strided_starts:
            results.append([w for _,w in windows_arr[start : start + context]])
            for s in range(start, start+context-1):
                if s < n - context+1:
                    keys_to_delete.add(windows_arr[s][0])

        with self._windows_lock:
            for k in keys_to_delete:
                self._windows.pop(k, None)

        return results


def process_window(sensors: dict[str, InferenceSensor], consumerService: KafkaConsumerService):
    logger.info(f"raw data consumer created")
    # read incoming kafka message
    consumer = consumerService.get_consumer()
    while True:
        msg = consumer.poll(1.0) 
        if msg is None:
            continue

        if msg.error():
            logger.warning("Consumer error (skipping data):", msg.error())
            continue

        message = json.loads(msg.value().decode("utf-8"))
        try:
            window = SeismicWindow(**message)
        except Exception as e:
            logger.warning("Failed to convert json to SeismicWindow object (skipping):", e)
            continue
        if window.key not in sensors:
            logger.warning(f"sensor {window.key} not found, skipping")
            continue
        sensor = sensors[window.key]

        # if window not 500, 3 when sampling rate = 100hz and window time 5 seconds. 3 means NZE or 3 channel, this for ensuring that data are exactly 6000,3
        expected_shape = (sensor.window_size, 3)
        if window.trace.shape != expected_shape:
            logger.warning(f"window shape not aligned {window.trace.shape} != {expected_shape} for data {window.starttime}-{window.endtime} ")
            continue

        if window.orientation_order != sensor.orientation:
            logger.warning(f"window channel not true {window.orientation_order} != {sensor.orientation} for window {window.starttime}-{window.endtime} skipping...")
            continue
        sensor.insert_window(window)
        logger.debug(f"window {sensor.key} {window.starttime} - {window.endtime} inserted")


def prediction_emitter(sensor: InferenceSensor, producerService: KafkaProducerService, eqt: EqTransformerService, topic_name:str ):
    logger.info(f"Prediction Producer for {sensor.key} created")
    producer = producerService.get_producer()
    while True:
        windows_batches = sensor.get_window_batches(6)

        if len(windows_batches) == 0:
            continue
        merged_traces = []
        window_infos: List[List[SeismicWindow]] = []

        for windows in windows_batches:
            traces = [w.trace for w in windows]
            merged_trace = np.concatenate(traces, axis=0)

            if merged_trace.shape != (6000,3):
                # Resample to match target shape
                target_fs = 100
                orig_fs = sensor.fs
                g = math.gcd(target_fs, orig_fs)
                up = target_fs // g
                down = orig_fs // g
                for i in range(len(traces)):
                    traces[i] = resample_poly(traces[i], up=up, down=down)
                merged_trace = np.concatenate(traces, axis=0)
                if merged_trace.shape != (6000, 3):
                    logger.warning(
                        f"sampling rate {sensor.fs}Hz not supported "
                        f"for {windows[0].key}, skipping..."
                    )
                    continue

            merged_traces.append(merged_trace)
            window_infos.append(windows)

        logger.info(f"predicting window {sensor.key} {windows_batches[0][0].starttime} - {windows_batches[-1][-1].endtime}")
        start = time.perf_counter()
        try:
            predictions = predict(
                model=eqt.model,
                seismic_traces=np.array(merged_traces),  # shape: (num_windows, 6000, 3)
                batch_size=16
            )
            end = time.perf_counter()
            elapsed = end - start
            logger.info(f"window {sensor.key} {windows_batches[0][0].starttime} - {windows_batches[-1][-1].endtime} predicted successfully in {'{:.2f}'.format(elapsed)} seconds")
        except Exception as e:
            logger.warning(f"window {sensor.key} {windows_batches[0][0].starttime} - {windows_batches[-1][-1].endtime} fail to predict")
            logger.warning("cause: ", e)
            logger.warning(f"skipping batches...")
            continue
            
        for i, windows in enumerate(window_infos):
            prediction = {
                "DD_mean": predictions["DD_mean"][i],
                "PP_mean": predictions["PP_mean"][i],
                "SS_mean": predictions["SS_mean"][i],
            }

            merged_trace = merged_traces[i]
            # row : Z, N, E, Detection probability (DD_mean), P-Pick Probability (PP_mean), S-Pick Probability (SS_mean)
            # guarantee to be n, 6000, 6
            merged_results = np.column_stack((
                merged_trace,
                prediction["DD_mean"],
                prediction["PP_mean"],
                prediction["SS_mean"],
            ))

            lengths = [t.shape[0] for t in [w.trace for w in windows]]
            indices = np.cumsum(lengths)
            sliced_results = np.split(merged_results, indices[:-1])
            if (len(sliced_results) != len(windows)):
                logger.warning(f"window {sensor.key} {window.starttime} - {window.endtime} data bug, skipping..")
                continue

            for j, window in enumerate(windows):
                res = sliced_results[j]
                for i, sample in enumerate(res):
                    milliseconds = i * 10 
                    ts = window.starttime + timedelta(milliseconds=milliseconds)

                    message = {
                        "key": sensor.key,
                        "model_name":eqt.model_name,
                        "ts": ts.isoformat(timespec="milliseconds"),
                        "network": sensor.network,
                        "station": sensor.station,
                        "sensor_code": sensor.sensor_code,
                        "orientation_order": sensor.orientation,
                        "trace1":float(sample[0]),
                        "trace2":float(sample[1]),
                        "trace3":float(sample[2]),
                        "dd": float(sample[3]),
                        "pp": float(sample[4]),
                        "ss": float(sample[5]),
                    }
                    producer.produce(topic=topic_name, value=json.dumps(message).encode("utf-8"))
                    producer.poll(0)
                logger.debug(f"sending to predict consumer {sensor.key}: {window.starttime} - {window.endtime}")
        time.sleep(2)
