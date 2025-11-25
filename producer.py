import json
import websocket
import rel
from kafka.admin import KafkaAdminClient, NewTopic 
from kafka.producer import KafkaProducer
from kafka.errors import TopicAlreadyExistsError
import threading

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092", 
)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  
    value_serializer=lambda v: json.dumps(v).encode('utf-8') 
)
topic_list = []
topic_list.append(NewTopic(name="blockchain", num_partitions=1, replication_factor=1))
try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
except TopicAlreadyExistsError:
    print("WARNING: Topic 'blockchain' already exists")

# track
lock = threading.Lock()
total_produce = 0

def on_message(ws, message):
    try:
        data = json.loads(message)
    except json.JSONDecodeError as e:
        return
    producer.send("blockchain", value=data)
    with lock:
        global total_produce
        total_produce += 1
        print(f"message sent: {total_produce}", end="\r", flush=True)
    producer.flush()


def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("### closed ###")

def on_open(ws):
    print("Opened connection")

if __name__ == "__main__":
    ws = websocket.WebSocketApp("wss://ws.blockchain.info/inv",
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)
    ws.run_forever(dispatcher=rel, reconnect=5) 
    ws.send("{\"op\": \"unconfirmed_sub\"}")
    rel.signal(2, rel.abort)
    rel.dispatch()