# CDC Consumer Client
This client will access the locally-run databases and subscribe to the relevant Kafka topics. The events will be processed and sent to `../data/staging_db.db`, which is a DuckDB file. If staging_db is not setup, this client will create it and create tables shown in `../data/staging-init.sql`.

## How to run
Uses UV for package management. Virtual environment setup:
```sh
uv venv
uv sync
.venv/Scripts/activate
```
To run the mock client simply:
```sh
python main.py
```

## Future work
Develop into docker image and run inside docker.