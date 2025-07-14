from pyflink.table import TableEnvironment
from common.config import (
    BOOTSTRAP_SERVERS,
    POSTGRES_DEBEZIUM_METADATA_CREATE_TABLE,
    POSTGRES_DEBEZIUM_METADATA_INSERT_INTO,
    MYSQL_DEBEZIUM_METADATA_CREATE_TABLE, 
    MYSQL_DEBEZIUM_METADATA_INSERT_INTO,
    DW_DATABASE,
    DW_HOSTNAME,
    DW_USERNAME,
    DW_PASSWORD,
    DW_PORT,
    DW_BRONZE_SCHEMA,
)
from abc import ABC, abstractmethod


class KafkaDataStreamReader(ABC):
    table_env: TableEnvironment
    bootstrap_servers: str
    debezium_metadata_create_table: str
    debezium_metadata_insert_into: str
    dw_database: str
    dw_hostname: str
    dw_username: str
    dw_password: str
    dw_port: str
    dw_bronze_schema: str
    base_table_name: str
    kafka_topic: str
    kafka_group: str
    is_materialized: bool

    def __init__(
        self,
        kafka_topic: str,
        base_table_name: str,
        source_database_type: str,
        table_env: TableEnvironment,
        kafka_group: str,
        is_materialized: bool
    ):
        """
        Creates base implementation of a data stream

        Args:
            kafka_topic (str): Kafka topic string for table being streamed
            base_table_name (str): Name of table in source system
            source_database_type (str): Source database
            table_env (TableEnvironment): Flink table environment
            kafka_group (str): Kafka group used for stream
            is_materialized (bool): Is feeding a materialized table in DW for bronze records
        """
        self.table_env = table_env
        self.kafka_topic = kafka_topic
        self.kafka_group = kafka_group
        self.base_table_name = base_table_name
        self.is_materialized = is_materialized

        self.bootstrap_servers = BOOTSTRAP_SERVERS
        self.dw_database = DW_DATABASE
        self.dw_hostname = DW_HOSTNAME
        self.dw_username = DW_USERNAME
        self.dw_password = DW_PASSWORD
        self.dw_port = DW_PORT
        self.dw_bronze_schema = DW_BRONZE_SCHEMA

        if source_database_type == 'postgres':
            self.debezium_metadata_create_table = (
                POSTGRES_DEBEZIUM_METADATA_CREATE_TABLE
            )
            self.debezium_metadata_insert_into = (
                POSTGRES_DEBEZIUM_METADATA_INSERT_INTO
            )
        elif source_database_type == 'mysql':
            self.debezium_metadata_create_table = (
                MYSQL_DEBEZIUM_METADATA_CREATE_TABLE
            )
            self.debezium_metadata_insert_into = (
                MYSQL_DEBEZIUM_METADATA_INSERT_INTO
            )
        else:
            raise Exception(f"Unrecognized database vendor {source_database_type}") 

        self.kafka_source_table_name = f"{base_table_name}_source"
        self.formatted_table_name = f"{base_table_name}_formatted"

        self.dw_connector = f""" (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://{DW_HOSTNAME}:{DW_PORT}/{DW_DATABASE}',
            'table-name' = '{DW_BRONZE_SCHEMA}.{self.base_table_name}_sink',
            'driver' = 'org.postgresql.Driver',
            'username' = '{DW_USERNAME}',
            'password' = '{DW_PASSWORD}'
        )"""

    def create_topic_table(self):
        self.table_env.execute_sql(f"""
            CREATE TABLE {self.kafka_source_table_name} (
                payload STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{self.kafka_topic}',
                'properties.bootstrap.servers' = '{self.bootstrap_servers}',
                'properties.group.id' = '{self.kafka_group}',
                'format' = 'json',
                'scan.startup.mode' = 'earliest-offset'
            );
        """)

    def insert_into_landing_table(self):
        parse_kafka_row_sql = self.parse_kafka_insert_rows()
        self.table_env.execute_sql(f"""
        INSERT INTO {self.formatted_table_name}
        SELECT 
            {parse_kafka_row_sql},
            {self.debezium_metadata_insert_into}
        FROM {self.kafka_source_table_name};""")

    def create_formatted_table(self):
        parse_kafka_row_sql = self.parse_kafka_create_rows()
        sql = f"""
        CREATE TABLE {self.formatted_table_name} (
            {parse_kafka_row_sql},
            {self.debezium_metadata_create_table}
        )
        """
        if self.is_materialized:
            sql += f"WITH {self.dw_connector}"
        sql += ';'
        
        self.table_env.execute_sql(sql)

    @abstractmethod
    def parse_kafka_create_rows(self):
        pass

    @abstractmethod
    def parse_kafka_insert_rows(self):
        pass