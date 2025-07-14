import os 
import sys
from dataclasses import dataclass

DW_DATABASE=os.environ['DW_DATABASE']
DW_USERNAME=os.environ['DW_USERNAME']
DW_PASSWORD=os.environ['DW_PASSWORD']
DW_HOSTNAME=os.environ['DW_HOSTNAME']
DW_PORT=os.environ['DW_PORT']
DW_BRONZE_SCHEMA='landing_db'
DW_SILVER_SCHEMA='staging_db'
BOOTSTRAP_SERVERS=os.environ['BOOTSTRAP_SERVERS']

STREAM_KAFKA_GROUP = 'etlStream-prod-1'

POSTGRES_DEBEZIUM_METADATA_CREATE_TABLE = """
op CHAR(1),
emitted_ts_ms BIGINT,
ts_ms BIGINT,
connector_version VARCHAR(20),
transaction_id VARCHAR(20),
lsn BIGINT
"""

POSTGRES_DEBEZIUM_METADATA_INSERT_INTO = """
CAST(JSON_VALUE(payload, '$.op') AS CHAR(1)) AS op,
CAST(JSON_VALUE(payload, '$.ts_ms') AS BIGINT) AS emitted_ts_ms,
CAST(JSON_VALUE(payload, '$.source.ts_ms') AS BIGINT) AS ts_ms,
CAST(JSON_VALUE(payload, '$.source.version') AS VARCHAR(20)) AS connector_version,
CAST(JSON_VALUE(payload, '$.source.txId') AS VARCHAR(20)) AS transaction_id,
CAST(JSON_VALUE(payload, '$.source.lsn') AS BIGINT) AS lsn
"""

MYSQL_DEBEZIUM_METADATA_CREATE_TABLE = """
op CHAR(1),
emitted_ts_ms BIGINT,
ts_ms BIGINT,
connector_version VARCHAR(20),
transaction_id VARCHAR(20)
"""

MYSQL_DEBEZIUM_METADATA_INSERT_INTO = """
CAST(JSON_VALUE(payload, '$.op') AS CHAR(1)) AS op,
CAST(JSON_VALUE(payload, '$.ts_ms') AS BIGINT) AS emitted_ts_ms,
CAST(JSON_VALUE(payload, '$.source.ts_ms') AS BIGINT) AS ts_ms,
CAST(JSON_VALUE(payload, '$.source.version') AS VARCHAR(20)) AS connector_version,
CAST(JSON_VALUE(payload, '$.source.gtid') AS VARCHAR(20)) AS transaction_id
"""