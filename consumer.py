import json
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from threading import Lock

cluster = Cluster(['127.0.0.1'], port=9042)  
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS blockchain
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS blockchain.transactions (
        tx_hash text PRIMARY KEY,
        lock_time int,
        ver int,
        size int,
        time bigint,
        vin_sz int,
        vout_sz int,
        relayed_by text,
        inputs text,
        outputs text
    )
""")

insert_query = session.prepare("""
    INSERT INTO blockchain.transactions (
        tx_hash, lock_time, ver, size, time, vin_sz, vout_sz, relayed_by, inputs, outputs
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

consumer = KafkaConsumer(
    'blockchain',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='scylla_consumer'
)

lock = Lock()
total_consumed = 0

def process_message(message):
    global total_consumed
    data = message.value

    x = data.get("x", {})
    tx_hash = x.get("hash")
    lock_time = x.get("lock_time")
    ver = x.get("ver")
    size = x.get("size")
    time_field = x.get("time")
    vin_sz = x.get("vin_sz")
    vout_sz = x.get("vout_sz")
    relayed_by = x.get("relayed_by")
    inputs = json.dumps(x.get("inputs", []))
    outputs = json.dumps(x.get("out", []))

    session.execute(insert_query, (
        tx_hash, lock_time, ver, size, time_field, vin_sz, vout_sz, relayed_by, inputs, outputs
    ))

    with lock:
        total_consumed += 1
        print(f"Messages consumed: {total_consumed}", end="\r", flush=True)

for msg in consumer:
    process_message(msg)
