import logging

from source.models import AppSettings, InfraServices
from source.infra import KafkaAdmin, KafkaConfig,KafkaProducerService, KafkaConsumerService, EqTransformerService, ClickHouseConfig, bootstrap_clickhouse, ScyllaService
from source.lib import SeqSelfAttention, LayerNormalization, FeedForward, f1 

logger = logging.getLogger("infra")

def bootstrap_infra(settings: AppSettings) -> InfraServices:
    logger.info("Initializing infrastructure services")

    try:
        logger.info("Initializing Kafka admin")
        kafka_admin = KafkaAdmin(settings.bootstrap_kafka)
        kafka_admin.create_topic(settings.window_seismic_topic)
        kafka_admin.create_topic(settings.prediction_seismic_topic)

        logger.info("Initializing ScyllaDB")
        scylla = ScyllaService(
            hosts=settings.scylla_hosts,
            port=settings.scylla_port,
            keyspace=settings.scylla_keyspace,
        )

        logger.info("Bootstrapping ClickHouse")
        clickhouse_config = ClickHouseConfig(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            kafka_broker=settings.clickhouse_bootstrap_kafka,
            kafka_topic=settings.prediction_seismic_topic,
            kafka_group=settings.clickhouse_kafka_group,
        )
        bootstrap_clickhouse(clickhouse_config)

        logger.info("Initializing Kafka producer")
        producer = KafkaProducerService(settings.bootstrap_kafka)

        logger.info("Initializing Kafka consumers")
        window_consumer = KafkaConsumerService(
            KafkaConfig(
                bootstrap_servers=settings.bootstrap_kafka,
                group_id=settings.raw_window_kafka_group,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            ),
            topics=[settings.window_seismic_topic],
        )

        prediction_consumer_scylla = KafkaConsumerService(
            KafkaConfig(
                bootstrap_servers=settings.bootstrap_kafka,
                group_id=settings.scylla_kafka_group,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            ),
            topics=[settings.prediction_seismic_topic],
        )

        logger.info("Loading EqTransformer model")
        eqt = EqTransformerService(
            model_path=settings.model_path,
            model_name=settings.model_name,
            custom_objects={
                "SeqSelfAttention": SeqSelfAttention,
                "LayerNormalization": LayerNormalization,
                "FeedForward": FeedForward,
                "f1": f1,
            },
            gpu_memory_limit=3000,
        )

        logger.info("Infrastructure initialization completed successfully")

        return InfraServices(
            kafka_admin=kafka_admin,
            producer=producer,
            window_consumer=window_consumer,
            prediction_consumer_scylla=prediction_consumer_scylla,
            scylla=scylla,
            eqt=eqt,
        )

    except Exception as exc:
        logger.exception("Infrastructure initialization failed")
        raise RuntimeError("Failed to initialize infrastructure") from exc
