import json
from datetime import datetime, timezone
import logging
from pydantic import BaseModel
from source.infra import KafkaConsumerService, ScyllaService
from cassandra.query import BatchStatement

logger = logging.getLogger("sink")

class PredictionResults(BaseModel):
    key: str
    model_name: str
    ts: datetime
    network: str
    station: str
    sensor_code: str
    orientation_order: str
    trace1: float
    trace2: float
    trace3: float
    dd: float
    pp: float
    ss: float


from collections import defaultdict

def start_scylla_sink(
    consumerService: KafkaConsumerService,
    scyllaService: ScyllaService,
    batch_size: int = 5000
):
    logger.info("scylla sink started")
    consumer = consumerService.get_consumer()

    batches = defaultdict(list)

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            logger.warning("Kafka error (skipping data): %s", msg.error())
            continue

        try:
            payload = json.loads(msg.value().decode("utf-8"))
            result = PredictionResults(**payload)
        except Exception as e:
            logger.warning("Invalid message (skipping data): %s", e)
            continue

        u_ts = result.ts.replace(tzinfo=timezone.utc)

        hour = (
            u_ts
            .astimezone(timezone.utc)
            .replace(minute=0, second=0, microsecond=0)
        )
        batch_key = (result.key, result.model_name, hour)
        batch = batches[batch_key]
        batch.append(result)

        if len(batch) >= batch_size:
            scyllaService.insert_predictions(batch)
            batch.clear()

