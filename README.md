# End-to-End Data Engineering Project
An end-to-end data engineering project involving all stages of the data engineering pipeline.
* MySQL & Postgres source databases
* Kafka & Debezium for CDC ELT
* DBT for transforming data
* Kimball's Dimensional modeling for data modeling

## Project Context
A book store is looking to analyze their book sales, inventory and shipping efficiency. They currently have two operational databases which manage their business processes. The Postgres database contains data on their store, and a MySQL database tracks their deliveries.

## Data Ingestion
Data is being ingested using Kafka and Debezium. These systems and the source databases will be run in docker. \
Apache Kafka is a distributed event streaming platform. It will allow us to capture real-time changes in the source systems. Another system can publish events to kafka, where another system can subscribe to the Kafka stream to receive those events. The publishing service will be Debezium. \
Debezium is a change data capture tool. It will publish data change events to Kafka. It does this by making use of logs created by the source system. These logs are part of an ACID database's durability guarantee, which varies based on the database's implementation. 
### Source System Configuration
#### Postgres - store_db
For Postgres, this is the write-ahead logs (WAL). For CDC systems, logical WALs are required to record the actual data changes. Lower percision in WAL will record just SQL statements, and not the actual data changes. This does have a slight performance impact on the database. This can enabled in docker compose:
```yaml
    command: [ "postgres", "-c", "wal_level=logical" ]
```
For additional information in CDC, this project also enables full replication on all of the postgres tables. Example:
```sql
ALTER TABLE customers REPLICA IDENTITY FULL;
```
This is done to get a full picture of the data before and after the change has occurred. Without this, the payload sent by Kafka does not have the "before" value for the data. This is important is it allows us to distinguish what has changed in an update. This allows us discard changes which don't contain any tracked changes.
#### MySQL - shipping_db
For MySQL, this is the Binary Log (binlogs). Again, the CDC system needs full row changes, not just statements. Binlogs are enabled and set to full row changes by the following mysql.conf:
```conf
[mysql]
server_id=223344
log_bin=/var/log/mysql/mysql-bin.log
binlog_format=ROW
binlog_row_image=FULL
```
This can be passed into the docker instance using the following in docker compose:
```yaml
- ./database-init-scripts/mysql/conf:/etc/mysql/conf.d
```
With these settings, it is now possible for Debezium to capture changes in the source databases.
> Versions are very important here. Recent versions made some changes to reading binlogs, it's important that Debezium is updated to make sure these changes can be accounted for.

> This setup is missing the global transaction id (gtid) field for MySQL. It's counterpart is present in the Postgres implementation by default. This ID can be useful for auditing data changes.

The database schemas for Postgres are shown in `database-init-scripts/*/init-scripts/init.sql`
### Debezium
Now everything is set up for Debezium. However, Debezium needs to know where the databases are and how to read them. It can know this by setting up connectors. This is done in the `utils/create_connector.py` script. Essentially it just posts configs to the Debezium instance.
For store_db, instance of Postgres:
```json
{
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
```
For shipping_db, instance of MySQL:
```json
{
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
```
### Kafka
With those Debezium settings, it will be able to send data to Kafka once changes are made to the databases. \
Once the first data events are sent to Kafka, it will create topics for each table. These are:
```
pg-changes.public.authors
pg-changes.public.orders
pg-changes.public.order_items
pg-changes.public.books
pg-changes.public.customers
mysql-changes.shipping_db.carriers
mysql-changes.shipping_db.shipment_events
mysql-changes.shipping_db.shipments
mysql-changes.shipping_db.shipping_services
```
### Kafka Consumer
A custom Kafka consumer was created in `consumer`.
This consumer is designed to subscribe to all the Kafka topics, receive data changes and write them to DuckDB after minor processing.
Processing includes interpreting the data schema from Debezium, filtering out columns not needed in data analysis and obfuscating PII. As this is a ELT set-up, the processing is mainly done after data has landed into the OLAP environment.
> The processing overhead could be improved if Avro was used.
### Mock Producer
A mock database editing client was created in `mock-db-client`.
This producer is designed to insert and update junk data into the databases. This is used for system testing, to make sure data changes are being captured correctly.

## Data Processing
After the data changes have landed into the staging.db (a DuckDB file) they are then processed into a Dimensional model using DBT. \
The data modeling is done using Kimball's Dimensional modeling.
### Enterprise Data Warehouse Bus Matrix
| Business Processes  | Grain                                 | Metrics       | Date | Books | Carrier Services | Customers | Orders | Shipments |
|---------------------|----------------------------------------|---------------|------|-------|------------------|-----------|--------|-----------|
| Book Inventory      | 1 row per book's stock level per day  | Stock count   |  X   |   X   |                  |           |        |           |
| Item Transactions   | 1 row per book in an order            | Sales         |  X   |   X   |                  |     X     |   X    |           |
| Shipment Events     | 1 row per shipment of an order        | Delivery time |  X   |       |        X         |           |   X    |     X     |

More information on Dimensions and Facts in DBT documentation in `modeling/book_store`
