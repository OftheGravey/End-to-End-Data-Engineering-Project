import requests
import json 
from time import sleep

base_url = 'http://localhost:8010'
headers = {'Content-Type': 'application/json'}
psql_connector_config = {
    "name": "postgresql-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "connector.displayName": "PostgreSQL",
      "topic.prefix": "pg-changes",
      "database.user": "postgres",
      "database.dbname": "store_db",
      "table.exclude.list": "audit",
      "database.hostname": "store-db",
      "database.password": "postgres",
      "name": "postgresql-connector",
      "connector.id": "postgres",
      "plugin.name": "pgoutput"
    }
}
mysql_connector_config = {
  "name": "mysql-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "topic.prefix": "mysql-changes",
    "database.hostname": "shipping-db",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "dbz",
    "database.server.id": "184054",
    "database.server.name": "shipping-db",
    "database.include.list": "shipping_db",
    "include.schema.changes": "true",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "schema-changes.mysql"
  }
}

if __name__ == "__main__":
    # Setup store_db connector
    json_data = json.dumps(psql_connector_config)
    res = requests.post(f"{base_url}/connectors", headers=headers, data=json_data)
    if res.status_code == 201:
        print("Connection created")
    elif res.status_code == 409:
        print("Connection already exists")
    else:
        raise Exception("Possible Failure,", res.status_code, res.reason)
    
    # Setup shipping_db connector
    json_data = json.dumps(mysql_connector_config)
    res = requests.post(f"{base_url}/connectors", headers=headers, data=json_data)
    if res.status_code == 201:
        print("Connection created")
    elif res.status_code == 409:
        print("Connection already exists")
    else:
        print(res.json())
        raise Exception("Possible Failure,", res.status_code, res.reason)

    print("Waiting 5 seconds for setup to complete.")
    sleep(5)

    # Check if connector were set up properly   
    res = requests.get(f"{base_url}/connectors/{psql_connector_config['name']}/status")
    if res.status_code != 200:
        raise Exception("Possible Failure,", res.status_code, res.reason)
    
    print("Connector active:", res.json()['connector']['state'])
    print(res.json())

    res = requests.get(f"{base_url}/connectors/{mysql_connector_config['name']}/status")
    if res.status_code != 200:
        raise Exception("Possible Failure,", res.status_code, res.reason)
    
    print("Connector active:", res.json()['connector']['state'])
    print(res.json())
    