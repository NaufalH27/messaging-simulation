from datetime import datetime, timedelta
import json
import math
import sys
import threading
import time
from typing import Dict, List, Set, Tuple
import numpy as np
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from obspy import UTCDateTime
import pandas as pd
from eqt_predict_api_tf29 import predict
import tensorflow as tf
from keras.models import load_model
from eqt_predict_api_tf29 import SeqSelfAttention, FeedForward, LayerNormalization, f1
from pydantic import BaseModel, ConfigDict, field_validator
from scipy.signal import resample_poly

WINDOW_SECONDS = 5
if 60 % WINDOW_SECONDS != 0:
    print(
        f"WINDOW_SECONDS ({WINDOW_SECONDS}) must divide 60 exactly"
    )
    sys.exit(1)

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

class Sensor:
    def __init__(
        self,
        key: str,
        network: str,
        station: str,
        fs: int,
        sensor_code: str,
        orientation: str,
    ):
        self.key = key
        self._windows: Dict[int, SeismicWindow] = {}
        self._windows_lock = threading.Lock()
        self.network = network
        self.station = station
        self.sensor_code = sensor_code
        self.orientation = orientation
        self.fs = fs
        self.window_size = WINDOW_SECONDS * fs

    def insert_window(self, window: SeismicWindow) -> None:
        delta_seconds = (window.starttime - UTCDateTime(0).datetime).total_seconds()
        if delta_seconds % WINDOW_SECONDS != 0:
            raise ValueError("unaligned window start")

        delta = int(delta_seconds // WINDOW_SECONDS)
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

        context = 60 // WINDOW_SECONDS
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


sensors: dict[str, Sensor] = {}

try :
    # MODELS
    print(f"intializing EqTransformer Model")
    MODEL_PATH = "artifacts/EqT_model_conservative.h5"
    MODEL_NAME = "EqTransformer-tf29"
    GPU_MEMORY_LIMIT=2000
    gpus = tf.config.experimental.list_physical_devices('GPU')
    if gpus:
        try:
            tf.config.experimental.set_virtual_device_configuration(
                gpus[0],
                [tf.config.experimental.VirtualDeviceConfiguration(memory_limit=GPU_MEMORY_LIMIT)]
            )
            print(f"Using GPU: {gpus[0].name}")
        except RuntimeError as e:
            print("Error setting GPU configuration:", e)
    else:
        print("No GPU found. Using CPU.")

    model = load_model(
        MODEL_PATH,
        custom_objects={
            'SeqSelfAttention': SeqSelfAttention,
            'FeedForward': FeedForward,
            'LayerNormalization': LayerNormalization,
            'f1': f1
        }
    )

    print(f"EqTransfomer model loaded ({MODEL_PATH})")

    # KAFKA
    print("connecting kafka")
    conf = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "seismic-consumer",
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(conf)
    consumer.subscribe(["seismic"])

    admin_client = AdminClient({
        "bootstrap.servers": "localhost:9092"
    })
    producer = Producer({
        "bootstrap.servers": "localhost:9092"
    })

    topic_name = "seismic-predictions"
    new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    fs = admin_client.create_topics([new_topic])

    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic} created")
        except Exception:
            print(f"INFO: Topic '{topic}' already exists")

except Exception as e:
    print("error initialization :", e)
    print("Terminating processes…")
    try:
        consumer.close()
    except Exception:
        pass
    sys.exit(1)

def raw_consumer(sensor: Sensor, consumer: Consumer):
    print(f"raw data consumer for {sensor.key} created")
    # read incoming kafka message
    while True:
        msg = consumer.poll(1.0) 
        if msg is None:
            continue

        if msg.error():
            print("Consumer error:", msg.error())
            continue

        message = json.loads(msg.value().decode("utf-8"))
        try:
            window = SeismicWindow(**message)
        except Exception as e:
            print("Failed to convert json to SeismicWindow object:", e)
            continue
        if window.key not in sensors:
            print(f"sensor {window.key} not found, skipping")
            continue

        # if window not 500, 3 when sampling rate = 100hz and window time 5 seconds. 3 means NZE or 3 channel, this for ensuring that data are exactly 6000,3
        expected_shape = (sensor.window_size, 3)
        if window.trace.shape != expected_shape:
            print(f"window shape not aligned {window.trace.shape} != {expected_shape} for data {window.starttime}-{window.endtime} ")
            print("rebuilding data buffer...")
            continue

        if window.orientation_order != sensor.orientation:
            print(f"window channel not true {window.orientation_order} != {sensor.orientation} for window {window.starttime}-{window.endtime} ")
            print("rebuilding data buffer...")
            continue
        sensor.insert_window(window)
        print(f"window {sensor.key} {window.starttime} - {window.endtime} inserted")


def prediction_producer(sensor: Sensor, producer: Producer):
    print(f"Prediction Producer for {sensor.key} created")
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
                    print(
                        f"sampling rate {sensor.fs}Hz not supported "
                        f"for {windows[0].key}, skipping..."
                    )
                    continue

            merged_traces.append(merged_trace)
            window_infos.append(windows)

        print(f"predicting window {sensor.key} {windows_batches[0][0].starttime} - {windows_batches[-1][-1].endtime}")
        start = time.perf_counter()
        try:
            predictions = predict(
                model=model,
                seismic_traces=np.array(merged_traces),  # shape: (num_windows, 6000, 3)
                batch_size=16
            )
            end = time.perf_counter()
            elapsed = end - start
            print(f"window {sensor.key} {windows_batches[0][0].starttime} - {windows_batches[-1][-1].endtime} predicted successfully in {'{:.2f}'.format(elapsed)} seconds")
        except Exception as e:
            print(f"window {sensor.key} {windows_batches[0][0].starttime} - {windows_batches[-1][-1].endtime} fail to predict")
            print("cause: ", e)
            print(f"skipping batches...")
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
                print(f"window {sensors.key} {window.starttime} - {window.endtime} data bug, skipping..")
                continue

            for j, window in enumerate(windows):
                res = sliced_results[j]
                message = {
                    "key": sensor.key,
                    "network": sensor.network,
                    "station": sensor.station,
                    "sensor_code": sensor.sensor_code,
                    "sampling_rate": sensor.fs,
                    "size_per_window": sensor.window_size,
                    "orientation_order": sensor.orientation,
                    "starttime": window.starttime.isoformat(),
                    "endtime": window.endtime.isoformat(),
                    "trace": res[:, 0:3].tolist(),
                    "dd": res[:, 3].tolist(),
                    "pp": res[:, 4].tolist(),
                    "ss": res[:, 5].tolist(),
                }

                producer.produce(topic_name, value=json.dumps(message).encode("utf-8"))
                producer.poll(0)
                print(f"sending to producer {sensor.key}: {window.starttime} - {window.endtime}")
        time.sleep(2)

if __name__ == "__main__":
    try:
        df = pd.read_csv("artifacts/station_list.csv")
        threads = []
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
            sensor = sensors[sensor_key] = Sensor(
                key=sensor_key,
                network=network,
                station=station,
                fs=fs,
                sensor_code=sensor_code,
                orientation=orientation
            )
            print(f"created sensor {sensor_key} ({orientation})")
            for key, s in sensors.items():
                rw = threading.Thread(
                    target=raw_consumer,
                    args=(sensor, consumer),
                    daemon=True
                )
                rw.start()
                threads.append(rw)
                pp = threading.Thread(
                    target=prediction_producer,
                    args=(sensor, producer),
                    daemon=True
                )
                pp.start()
                threads.append(pp)
        print("EVERYTHING STARTED")
        while True: 
            time.sleep(1)

    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt, stopping…")
        sys.exit(0)
    finally:
        print("Terminating processes…")
        try:
            consumer.close()
            producer.flush()
        except Exception:
            pass
        sys.exit(0)


