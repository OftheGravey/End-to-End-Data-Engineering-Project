import requests
import json 

base_url = 'http://localhost:8083'
headers = {'Content-Type': 'application/json'}
connector_config = {
    "name": "postgresql-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "connector.displayName": "PostgreSQL",
      "topic.prefix": "pg-changes",
      "database.user": "postgres",
      "database.dbname": "db_local",
      "table.exclude.list": "audit",
      "database.hostname": "database",
      "database.password": "postgres",
      "name": "postgresql-connector",
      "connector.id": "postgres",
      "plugin.name": "pgoutput"
    }
}

if __name__ == "__main__":
    json_data = json.dumps(connector_config)
    res = requests.post(f"{base_url}/connectors", headers=headers, data=json_data)

    if res.status_code == 201:
        print("Connection created")
    elif res.status_code == 409:
        print("Connection already exists")
    else:
        raise Exception("Possible Failure,", res.status_code, res.reason)
        
    res = requests.get(f"{base_url}/connectors/{connector_config['name']}/status")

    if res.status_code != 200:
        raise Exception("Possible Failure,", res.status_code, res.reason)
    
    print("Connector active:", res.json()['connector']['state'])

    