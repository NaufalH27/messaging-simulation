import io
import json
import time
from typing import Optional, Type, TypeVar, Generic, List, Tuple
from minio import Minio
import numpy as np
from confluent_kafka import Consumer
from eqt_predict_tf29 import predict
import tensorflow as tf
from keras.models import load_model
from eqt_predict_tf29 import SeqSelfAttention, FeedForward, LayerNormalization, f1
from collections import deque
from pydantic import BaseModel, ConfigDict, field_validator
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

#MinIo
minio_client = Minio("localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)
MINIO_BUCKET = "seismic"
if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)

def upload_npz_to_minio(bucket, object_name, **arrays):
    """
    Upload multiple NumPy arrays as a single .npz file to MinIO.

    Example:
        upload_npz_to_minio(
            bucket,
            "XLMG08/20251216152052/window.npz",
            trace=trace_array,
            pp=pp_array,
            ss=ss_array,
            dd=dd_array
        )
    """
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

print(f"minio client created with bucket : {MINIO_BUCKET}")


# Scylla
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
        network text,
        station text,
        starttime timestamp,
        endtime timestamp,
        sampling_rate double,
        num_channels int,
        samples_per_channel int,
        channel_order text, 
        minio_ref text,
        PRIMARY KEY ((network, station), starttime)
    ) WITH CLUSTERING ORDER BY (starttime ASC);
""")



print("Scylla table window_prediction loaded")



# MODELS
MODEL_PATH = "components/EqT_model_original.h5"
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
conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "seismic-consumer",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(conf)
consumer.subscribe(["seismic"])

print("kafka seismic consumer started")


class SeismicWindow(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    network: str
    station: str
    starttime: datetime
    endtime: datetime
    sampling_rate: float
    data: np.ndarray
    samples_per_channel: int
    channel_order: Tuple[str, ...]
    num_channels: int
    total_samples: int
    sample_counts: Tuple[int, ...]

    @field_validator('data', mode='before')
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

WINDOW_SECONDS = 4
RING_BUFFER_LENGTH = 60 / WINDOW_SECONDS  # seconds / window

if RING_BUFFER_LENGTH % 1 != 0:
    raise ValueError(
        f"WINDOW_SECONDS={WINDOW_SECONDS} does not evenly divide 60 seconds.\n"
        "EQTransformer (EQT) requires input seismogram chunks to be exactly 60 seconds in length.\n"
        "Your sliding windows must evenly tile 60 seconds for the model to work correctly.\n"
        "Please choose a WINDOW_SECONDS value that is a factor of 60 (e.g., 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60)."
    )
rb = RingBuffer(maxlen=int(RING_BUFFER_LENGTH),dtype=SeismicWindow)

try :
    print("EVERYTHING STARTED")
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
        
        # if window not 400, 3
        expected_shape = (int(WINDOW_SECONDS * window.sampling_rate),3)
        if window.data.shape != expected_shape:
            print(f"window shape not aligned {window.data.shape} != {expected_shape} for data {window.starttime}-{window.endtime} ")
            print("rebuilding data buffer...")
            rb.delete_all()
            continue

        if window.channel_order != ("Z", "N", "E"):
            print(f"window channel not true {window.channel_order} != ZNE for data {window.starttime}-{window.endtime} ")
            print("rebuilding data buffer...")
            rb.delete_all()
            continue

        if len(rb) == 0:
            rb.append(window)
        else:
            latest = rb.get(-1)
            if window.starttime == latest.endtime:
                rb.append(window)
            elif window.starttime > latest.endtime:
                print(f"data not aligned {window.starttime} > {latest.endtime}")
                print("rebuilding data buffer...")
                rb.delete_all()
                continue
            elif window.starttime < latest.endtime:
                continue # ignore late window
        if rb.get_len() == rb.maxlen:
            merged_trace = np.concatenate([rb.get(i).data for i in range(len(rb))], axis=0)

            print(f"predicting window {window.starttime} - {window.endtime}")
            start = time.perf_counter()
            preds = predict(model=model, seismic_trace=merged_trace)
            end = time.perf_counter()
            elapsed = end - start
            print(f"window {window.starttime} - {window.endtime} predicted successfully in {elapsed}")
            pp_window = preds["PP_mean"][-400:]
            ss_window = preds["SS_mean"][-400:]
            dd_window = preds["DD_mean"][-400:]
            timestamp = window.starttime.strftime("%Y%m%d%H%M%S")
            dirname = f"{window.network}{window.station}"
            minio_ref = f"{dirname}/{timestamp}_window.npz"

            upload_npz_to_minio(
                MINIO_BUCKET,
                minio_ref,
                trace=window.data,
                pp=pp_window,
                ss=ss_window,
                dd=dd_window
            )

            insert_stmt = session.prepare("""
                INSERT INTO window_predictions (
                    network, station, starttime, endtime,
                    sampling_rate, num_channels, samples_per_channel,
                    channel_order, minio_ref
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)

            channel_order_str = ",".join(window.channel_order) if window.channel_order else None

            session.execute(
                insert_stmt,
                (
                    window.network,
                    window.station,
                    window.starttime,
                    window.endtime,
                    window.sampling_rate,
                    window.num_channels,
                    window.samples_per_channel,
                    channel_order_str,
                    minio_ref
                )
            )


except KeyboardInterrupt:
    print("Caught KeyboardInterrupt, stopping…")
finally:
    print("Terminating processes…")
    consumer.close()


