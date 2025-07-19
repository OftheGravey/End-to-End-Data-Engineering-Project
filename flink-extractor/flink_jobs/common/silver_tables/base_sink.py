from pyflink.table import StreamTableEnvironment
from abc import ABC, abstractmethod
from pyflink.datastream import StreamExecutionEnvironment, DataStream
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.jdbc import JdbcConnectionOptions
from common.jdbc_sink import JdbcSink
from datetime import datetime
import uuid
from common.config import (
    END_TIMESTAMP,
    DW_DATABASE,
    DW_HOSTNAME,
    DW_USERNAME,
    DW_PASSWORD,
    DW_PORT,
    DW_SILVER_SCHEMA,
)


def uuid_gen():
    return str(uuid.uuid4())


SOURCE_SCHEMA = Types.TUPLE(
    [
        Types.INT(),  # order_id
        Types.INT(),  # customer_id
        Types.SQL_TIMESTAMP(),  # order_date
        Types.STRING(),  # status
        Types.STRING(),  # shipping_method
        Types.STRING(),  # op
        Types.BIG_INT(),  # emitted_ts_ms
        Types.BIG_INT(),  # ts
        Types.STRING(),  # connector_version
        Types.STRING(),  # transaction_id
        Types.BIG_INT(),  # lsn
    ]
)

TARGET_SCHEMA = Types.TUPLE(
    [
        Types.INT(),  # order_id
        Types.STRING(),  # status
        Types.STRING(),  # shipping_method
        Types.SQL_DATE(),  # order_date
        Types.STRING(),  # order_sk
        Types.INT(),  # valid_from
        Types.INT(),  # valid_to
        Types.BOOLEAN(),  # current
    ]
)


class SCD2ProcessFunction(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.state = runtime_context.get_state(
            ValueStateDescriptor(
                "current_version",
                TARGET_SCHEMA,
            )
        )

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        # Incoming value
        (
            order_id,
            customer_id,
            order_date,
            status,
            shipping_method,
            op,
            emitted_ts_ms,
            ts_ms,
            connector_version,
            transaction_id,
            lsn,
        ) = value
        # Current value
        current = self.state.value()

        if current is None:
            # No existing version, insert new
            new_version = (
                order_id,
                status,
                shipping_method,
                order_date,
                uuid_gen(),  # order_sk
                ts_ms,
                END_TIMESTAMP,
                True,
            )
            self.state.update(new_version)
            yield new_version
        else:
            (
                cur_order_id,
                cur_status,
                cur_shipping_method,
                cur_order_date,
                cur_order_sk,
                cur_valid_from,
                cur_valid_to,
                cur_current,
            ) = current
            # Check if any value has changed
            new_compare = (status, shipping_method, order_date)
            curr_compare = (cur_status, cur_shipping_method, cur_order_date)
            is_different = not all(
                new_compare[i] == curr_compare[i] for i in range(new_compare)
            )
            if is_different:
                # Emit expired record
                expired_record = (
                    cur_order_id,
                    cur_status,
                    cur_shipping_method,
                    cur_order_date,
                    cur_order_sk,
                    cur_valid_from,
                    ts_ms - 1,
                    False,
                )
                yield expired_record

                # Emit new version
                new_version = (
                    order_id,
                    status,
                    shipping_method,
                    order_date,
                    uuid_gen(),  # new synthetic id
                    ts_ms,
                    END_TIMESTAMP,
                    True,
                )
                self.state.update(new_version)
                yield new_version


class BaseDataSink(ABC):
    t_env: StreamTableEnvironment
    dw_database: str
    dw_hostname: str
    dw_username: str
    dw_password: str
    dw_port: str
    schema_name: str
    dw_bronze_schema: str
    table_name: str

    def __init__(self, table_name: str, t_env: StreamTableEnvironment):
        self.t_env = t_env
        self.table_name = table_name
        self.dw_database = DW_DATABASE
        self.dw_hostname = DW_HOSTNAME
        self.dw_username = DW_USERNAME
        self.dw_password = DW_PASSWORD
        self.dw_port = DW_PORT
        self.schema_name = DW_SILVER_SCHEMA

    def insert_records(self):
        raw_table = self.t_env.from_path("orders_formatted")

        raw_stream: DataStream = self.t_env.to_data_stream(raw_table)

        processed_stream = raw_stream.key_by(lambda x: x[0]).process(
            SCD2ProcessFunction()
        )

        jdbc_sink = JdbcSink.sink(
            sql=f"""
            INSERT INTO {self.schema_name}.d_orders (
                order_id, status, shipping_method, order_date, order_sk, valid_from, valid_to, current
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (order_id, valid_from) DO UPDATE SET 
                status = EXCLUDED.status,
                shipping_method = EXCLUDED.shipping_method,
                order_date = EXCLUDED.order_date,
                valid_to = EXCLUDED.valid_to,
                current = EXCLUDED.current 
            """,
            type_info=TARGET_SCHEMA,
            jdbc_connection_options=JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url(
                f"jdbc:postgresql://{self.dw_hostname}:{self.dw_port}/{self.dw_database}"
            )
            .with_driver_name("org.postgresql.Driver")
            .with_user_name(self.dw_username)
            .with_password(self.dw_password)
            .build(),
        )

        processed_stream.add_sink(jdbc_sink)
