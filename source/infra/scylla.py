from threading import Semaphore
from cassandra.cluster import Cluster
from cassandra.cluster import Cluster
from datetime import datetime, timezone
from cassandra.util import uuid_from_time

class ScyllaService:
    def __init__(self, hosts: list[str], port: int, keyspace: str):
        print("Connecting to ScyllaDB")
        self._keyspace = keyspace
        self.cluster = Cluster(hosts, port=port)
        self.session = self.cluster.connect()
        self._init_schema()
        self._prepare_statements()
        self.max_inflight = 128
        self.write_sem = Semaphore(self.max_inflight)

    def _init_schema(self) -> None:
        self.session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {self._keyspace}
            WITH replication = {{
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }};
            """
        )
        self.session.set_keyspace(self._keyspace)

        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS predicted_samples (
                key text,
                model_name text,
                hour timestamp,
                ts timestamp,
                network text,
                station text,
                sensor_code text,
                orientation_order text,
                trace1 float,
                trace2 float,
                trace3 float,
                dd float,
                pp float,
                ss float,
                created_at timeuuid,
                PRIMARY KEY ((key, model_name, hour), ts, created_at)
            );
            """
        )

        self.session.execute(
            """
                CREATE TABLE IF NOT EXISTS predicted_sample_bucket_by_day (
                    key text,
                    model_name text,
                    hour timestamp,
                    PRIMARY KEY ((key, model_name), hour)
                ) WITH CLUSTERING ORDER BY (hour DESC);
            """
        )

    def _prepare_statements(self) -> None:
        self.insert_stmt = self.session.prepare(
            """
            INSERT INTO predicted_samples (
                key, model_name, hour, ts, 
                network, station, sensor_code,
                orientation_order,
                trace1, trace2, trace3,
                dd, pp, ss,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        self.insert_day_index = self.session.prepare(
            """
            INSERT INTO predicted_sample_bucket_by_day (key, model_name, hour)
            VALUES (?, ?, ?)
            """
        )

    def insert_predictions(self, predictions) -> None:
        if not predictions:
            return

        futures = []
        hours = set()
        for p in predictions:
            self.write_sem.acquire()
            p.ts = p.ts.replace(tzinfo=timezone.utc)
            hour = (
                p.ts
                .astimezone(timezone.utc)
                .replace(minute=0, second=0, microsecond=0)
            )
            hours.add((p.key, p.model_name, hour))
            created_at = uuid_from_time(datetime.now(timezone.utc))
            future = self.session.execute_async(
                self.insert_stmt,
                (
                    p.key,
                    p.model_name,
                    hour,
                    p.ts,
                    p.network,
                    p.station,
                    p.sensor_code,
                    p.orientation_order,
                    p.trace1,
                    p.trace2,
                    p.trace3,
                    p.dd,
                    p.pp,
                    p.ss,
                    created_at,
                ),
            )
            future.add_callbacks(
                callback=self._on_write_done,
                errback=self._on_write_done,
            )

            futures.append(future)

        for key, model_name, hour in hours:
            self.write_sem.acquire()
            future = self.session.execute_async(
                self.insert_day_index,
                (key, model_name, hour),
            )
            future.add_callbacks(
                callback=self._on_write_done,
                errback=self._on_write_done,
            )
            futures.append(future)
    def _on_write_done(self, *_):
        self.write_sem.release()