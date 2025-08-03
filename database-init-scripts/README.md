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