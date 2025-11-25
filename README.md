# messaging-simulation
messaging simulation using kafka + scylla

data stream : https://www.blockchain.com/explorer/api/api_websocket

install requirement python package:
```
  pip install -r requirements
```

run docker compose :
```
docker-compose -f docker-compose.kafka.yaml -f docker-compose.scylla.yaml up -d
```

run the script:
```
python producer.py
python consumer.py
```
