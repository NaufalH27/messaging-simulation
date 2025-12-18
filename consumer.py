import asyncio
import io
import json
import math
import sys
import time
from typing import Type, TypeVar, Generic, List, Tuple
from minio import Minio
import nats
import numpy as np
from confluent_kafka import Consumer
import pandas as pd
from eqt_predict_tf29 import predict
import tensorflow as tf
from keras.models import load_model
from eqt_predict_tf29 import SeqSelfAttention, FeedForward, LayerNormalization, f1
from collections import deque
from pydantic import BaseModel, ConfigDict, field_validator
from datetime import datetime, timezone
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from scipy.signal import resample_poly


WINDOW_SECONDS = 4
RING_BUFFER_LENGTH = int(60 / WINDOW_SECONDS)  # seconds / window

class SeismicWindow(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    key:str
    network: str
    station: str
    sensor_code:str
    starttime: datetime
    endtime: datetime
    sampling_rate: float
    trace: np.ndarray
    samples_per_channel: int
    orientation_order: Tuple[str, ...]
    num_channels: int
    total_samples: int
    sample_counts: Tuple[int, ...]

    @field_validator('trace', mode='before')
    @classmethod
    def convert_data_to_numpy(cls, v):
        return np.array(v, dtype=np.float32)

T = TypeVar("T")  
class RingBuffer(Generic[T]):
    def __init__(self, maxlen: int, dtype: Type[T]):
        self.maxlen = maxlen
        self._buffer = deque(maxlen=maxlen)
        self._dtype = dtype

    def append(self, item: T):
        if not isinstance(item, self._dtype):
            raise TypeError(f"RingBuffer only accepts {self._dtype.__name__}, got {type(item).__name__}")
        self._buffer.append(item)

    def get(self, idx: int) -> T:
        item = self._buffer[idx]
        if not isinstance(item, self._dtype):
            raise TypeError(f"Expected {self._dtype.__name__}, got {type(item).__name__}")
        return item

    def get_all(self) -> List[T]:
        return list(self._buffer)
    
    def get_len(self):
        return len(self._buffer)

    def delete_all(self):
        self._buffer = deque(maxlen=self.maxlen)

    def __len__(self):
        return len(self._buffer)

    def __iter__(self):
        return iter(self._buffer)

    def __repr__(self):
        return repr(self._buffer)

class Sensor:
    def __init__(self, network:str, station:str, fs:int, sensor_code:str , orientation:str):
        self.window_rb = RingBuffer(maxlen=RING_BUFFER_LENGTH, dtype=SeismicWindow)
        self.network = network
        self.station = station
        self.sensor_code = sensor_code # XX
        self.orientation = orientation #ZNE
        self.fs = fs

sensors: dict[str, Sensor] = {}


try :
    df = pd.read_csv("station_list.csv")
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
        sensors[sensor_key] = Sensor(
            network=network,
            station=station,
            fs=fs,
            sensor_code=sensor_code,
            orientation=orientation
        )
        print(f"created sensor {sensor_key} ({orientation})")

        #MinIo
        print(f"connecting minio")
        minio_client = Minio("localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
        MINIO_BUCKET = "seismic"
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)


    print(f"minio client started with bucket : {MINIO_BUCKET}")

    # Scylla
    print(f"connecting scylladb")
    cluster = Cluster(['127.0.0.1'], port=9042)  
    session = cluster.connect()
    KEYSPACE = "seismic_data"
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
    """)

    session.set_keyspace(KEYSPACE)
    session.execute("""
        CREATE TABLE IF NOT EXISTS window_predictions (
            key text,
            starttime timestamp,
            endtime timestamp,
            network text,
            station text,
            sensor_code text,
            sampling_rate double,
            num_channels int,
            samples_per_channel int,
            orientation_order text,
            total_samples int,
            sample_counts text,
            minio_ref text,
            created_at timestamp,
            model_name text,
            PRIMARY KEY ((key), starttime)
        ) WITH CLUSTERING ORDER BY (starttime ASC);
    """)

    print("ScyllaDB loaded")

    # MODELS
    print(f"intializing EqTransformer Model")
    MODEL_PATH = "components/EqT_model_conservative.h5"
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


except Exception as e:
    print("error initialization :", e)
    print("Terminating processes…")
    try:
        consumer.close()
    except Exception:
        pass
    sys.exit(1)

def upload_npz_to_minio(bucket, object_name, **arrays):
    buffer = io.BytesIO()
    np.savez(buffer, **arrays)
    buffer.seek(0)
    data = buffer.getvalue()
    minio_client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=io.BytesIO(data),
        length=len(data),
        content_type="application/octet-stream"
    )

async def main():
    # NATS
    nc = await nats.connect("localhost:4222")
    print("EVERYTHING STARTED")
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
        sensor = sensors[window.key]
        
        # if window not 400, 3 when sampling rate = 100hz, 3 means NZE or 3 channel, this for ensuring that data are exactly 6000,3
        expected_shape = (WINDOW_SECONDS * sensor.fs, len(sensor.orientation))
        if window.trace.shape != expected_shape:
            print(f"window shape not aligned {window.trace.shape} != {expected_shape} for data {window.starttime}-{window.endtime} ")
            print("rebuilding data buffer...")
            sensor.window_rb.delete_all()
            continue

        if window.orientation_order != tuple(sensor.orientation):
            print(f"window channel not true {window.orientation_order} != {sensor.orientation} for window {window.starttime}-{window.endtime} ")
            print("rebuilding data buffer...")
            sensor.window_rb.delete_all()
            continue

        if len(sensor.window_rb) == 0:
            sensor.window_rb.append(window)
        else:
            latest = sensor.window_rb.get(-1)
            if window.starttime == latest.endtime:
                sensor.window_rb.append(window)
            elif window.starttime > latest.endtime:
                print(f"data not aligned {window.starttime} > {latest.endtime}")
                print("rebuilding data buffer...")
                sensor.window_rb.delete_all()
                continue
            elif window.starttime < latest.endtime:
                continue # ignore late window

        if sensor.window_rb.get_len() == sensor.window_rb.maxlen:
            windows = sensor.window_rb.get_all()
            # trace in traces guarantee the same size
            traces = [w.trace for w in windows]
            merged_trace = np.concatenate(traces, axis=0)
            if merged_trace.shape != (6000,3):
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
                        f"for {window.key}, skipping..."
                    )
                    continue

            print(f"predicting window {window.key} {window.starttime} - {window.endtime}")
            start = time.perf_counter()
            try:
                prediction = predict(model=model, seismic_trace=merged_trace)
                # row : Z, N, E, Detection probability (DD_mean), P-Pick Probability (PP_mean), S-Pick Probability (SS_mean)
                # guarantee to be 6000, 6
                merged_results = np.column_stack((
                    merged_trace,
                    prediction["DD_mean"],
                    prediction["PP_mean"],
                    prediction["SS_mean"],
                ))
                
            except Exception as e:
                print(f"window {window.key} {window.starttime} - {window.endtime} fail to predict")
                print("cause: ", e)
                print(f"skipping window...")
            lengths = [t.shape[0] for t in traces]
            indices = np.cumsum(lengths)
            sliced_results = np.split(merged_results, indices[:-1])
            
            end = time.perf_counter()
            elapsed = end - start
            print(f"window {window.starttime} - {window.endtime} predicted successfully in {'{:.2f}'.format(elapsed)} seconds")
            minio_refs = {}

            for i, res in enumerate(sliced_results):
                w = windows[i]
                minio_ref = (
                    f"{w.key}/"
                    f"{w.starttime.strftime('%Y%m%d%H%M%S')}_"
                    f"{w.endtime.strftime('%Y%m%d%H%M%S')}.npz"
                )
                upload_npz_to_minio(
                    MINIO_BUCKET,
                    minio_ref,
                    trace=res[:, 0:3],   # Z, N, E
                    dd=res[:, 3],        # Detection
                    pp=res[:, 4],        # P-pick
                    ss=res[:, 5],        # S-pick
                )

                minio_refs[w.starttime.isoformat()] = minio_ref

            batch = BatchStatement()
            insert_stmt = session.prepare("""
                INSERT INTO window_predictions (
                    key, starttime, endtime,
                    network, station, sensor_code,
                    sampling_rate, num_channels,
                    samples_per_channel, total_samples,
                    orientation_order,
                    minio_ref,
                    model_name,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)
            now = datetime.now(timezone.utc)
            for w in windows:
                minio_ref = minio_refs[w.starttime.isoformat()]
                if minio_ref:
                    batch.add(
                        insert_stmt,
                        (
                            w.key,
                            w.starttime,
                            w.endtime,

                            sensor.network,
                            sensor.station,
                            sensor.sensor_code,

                            w.sampling_rate,
                            w.num_channels,
                            w.samples_per_channel,
                            w.total_samples,
                            "".join(w.orientation_order),
                            minio_ref,
                            MODEL_NAME,
                            now
                        )
                    )
            session.execute(batch)

            event = {
                "key": window.key,
                "starttime": window.starttime.isoformat(),
                "endtime": window.endtime.isoformat(),
                "minio_ref": minio_ref,
            }

            await nc.publish(
                subject=f"seismic.window.predicted.{sensor.network}.{sensor.station}.{sensor.sensor_code}",
                payload=json.dumps(event).encode("utf-8")
            )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt, stopping…")
    finally:
        print("Terminating processes…")
        try:
            consumer.close()
        except Exception:
            pass

