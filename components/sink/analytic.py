import io
from minio import Minio
from datetime import datetime, timezone
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

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

def main():
    minio_refs = {}
    lengths = [t.shape[0] for t in traces]
    indices = np.cumsum(lengths)
    sliced_results = np.split(merged_results, indices[:-1])

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