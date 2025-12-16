# messaging-simulation
messaging simulation using kafka + scylla

data stream : https://www.blockchain.com/explorer/api/api_websocket

install requirement python package:
```
  pip install -r requirements.txt
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

# training

install EQTransformer :

 = create and start venv (must be 3.10.5) :
 ```
 python -3.10 -m venv eqt_venv
 eqt_venv\Scripts\activate # (windows)
 source eqt_venv/bin/activate # (linux/MacOS)
 ```

 - run this line :
 ```
 pip install tensorflow==2.9.0 keras==2.9.0 numpy==1.22.4 scipy==1.10.0 matplotlib==3.5.2 pandas==1.4.3 h5py==3.6.0 obspy==1.3.0 tqdm==4.64.0 jupyter==1.0.0 pytest==7.1.2 keyring==23.7.0 pkginfo==1.8.3 
 ``` 
 - install EQTransformer 0.1.61 without dependencies :
 ```
 pip install git+https://github.com/smousavi05/EQTransformer --no-deps
 ``` 