"""
PyFlink DataStream job: Kafka JSON events -> PostgreSQL star schema.

The pipeline intentionally uses the DataStream API. SQL strings below are only
PostgreSQL prepared statements for JdbcSink, not Flink SQL/Table API.
"""

import json
import os

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.jdbc import (
    JdbcConnectionOptions,
    JdbcExecutionOptions,
    JdbcSink,
)
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sales")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "flink-sales-datastream")

POSTGRES_JDBC_URL = os.environ.get("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/lab")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "lab")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "lab")


def as_text(value):
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def as_int(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def as_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_event(message):
    try:
        event = json.loads(message)
    except json.JSONDecodeError:
        return None

    if not isinstance(event, dict):
        return None

    return event


def has_required_ids(event):
    return (
        event is not None
        and as_int(event.get("id")) is not None
        and as_int(event.get("sale_customer_id")) is not None
        and as_int(event.get("sale_seller_id")) is not None
        and as_int(event.get("sale_product_id")) is not None
    )


def to_customer_row(event):
    return Row(
        as_int(event.get("sale_customer_id")),
        as_text(event.get("customer_first_name")),
        as_text(event.get("customer_last_name")),
        as_int(event.get("customer_age")),
        as_text(event.get("customer_email")),
        as_text(event.get("customer_country")),
        as_text(event.get("customer_postal_code")),
        as_text(event.get("customer_pet_type")),
        as_text(event.get("customer_pet_name")),
        as_text(event.get("customer_pet_breed")),
    )


def to_seller_row(event):
    return Row(
        as_int(event.get("sale_seller_id")),
        as_text(event.get("seller_first_name")),
        as_text(event.get("seller_last_name")),
        as_text(event.get("seller_email")),
        as_text(event.get("seller_country")),
        as_text(event.get("seller_postal_code")),
    )


def to_product_row(event):
    return Row(
        as_int(event.get("sale_product_id")),
        as_text(event.get("product_name")),
        as_text(event.get("product_category")),
        as_float(event.get("product_price")),
        as_float(event.get("product_weight")),
        as_text(event.get("product_color")),
        as_text(event.get("product_size")),
        as_text(event.get("product_brand")),
        as_text(event.get("product_material")),
        as_text(event.get("product_description")),
        as_float(event.get("product_rating")),
        as_int(event.get("product_reviews")),
        as_text(event.get("product_release_date")),
        as_text(event.get("product_expiry_date")),
        as_text(event.get("pet_category")),
    )


def to_store_row(event):
    return Row(
        as_text(event.get("store_name")),
        as_text(event.get("store_location")),
        as_text(event.get("store_city")),
        as_text(event.get("store_state")),
        as_text(event.get("store_country")),
        as_text(event.get("store_phone")),
        as_text(event.get("store_email")),
    )


def to_supplier_row(event):
    return Row(
        as_text(event.get("supplier_name")),
        as_text(event.get("supplier_contact")),
        as_text(event.get("supplier_email")),
        as_text(event.get("supplier_phone")),
        as_text(event.get("supplier_address")),
        as_text(event.get("supplier_city")),
        as_text(event.get("supplier_country")),
    )


def to_fact_row(event):
    return Row(
        as_int(event.get("id")),
        as_text(event.get("sale_date")),
        as_int(event.get("sale_customer_id")),
        as_int(event.get("sale_seller_id")),
        as_int(event.get("sale_product_id")),
        as_int(event.get("sale_quantity")),
        as_float(event.get("sale_total_price")),
    )


def row_key(*values):
    return "\x1f".join("" if value is None else str(value) for value in values)


class EmitFirstSeen(KeyedProcessFunction):
    def __init__(self, state_name):
        self.state_name = state_name

    def open(self, runtime_context):
        self.seen = runtime_context.get_state(
            ValueStateDescriptor(self.state_name, Types.BOOLEAN())
        )

    def process_element(self, value, ctx):
        if self.seen.value():
            return

        self.seen.update(True)
        yield value


def deduplicate(stream, key_selector, key_type, row_type, name):
    return (
        stream.key_by(key_selector, key_type=key_type)
        .process(EmitFirstSeen(f"{name}-seen"), output_type=row_type)
        .name(name)
    )


def jdbc_options():
    return (
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(POSTGRES_JDBC_URL)
        .with_driver_name("org.postgresql.Driver")
        .with_user_name(POSTGRES_USER)
        .with_password(POSTGRES_PASSWORD)
        .build()
    )


def jdbc_execution_options():
    return (
        JdbcExecutionOptions.builder()
        .with_batch_size(500)
        .with_batch_interval_ms(1000)
        .with_max_retries(5)
        .build()
    )


def jdbc_sink(sql, row_type):
    return JdbcSink.sink(
        sql,
        row_type,
        jdbc_options(),
        jdbc_execution_options(),
    )


def make_postgres_upsert(table_name, columns, conflict_columns, update_columns=None):
    insert_columns = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    conflict_target = ", ".join(conflict_columns)

    if update_columns is None:
        update_columns = [column for column in columns if column not in conflict_columns]

    if not update_columns:
        return (
            f"INSERT INTO {table_name} ({insert_columns}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_target}) DO NOTHING"
        )

    update_clause = ", ".join(
        f"{column} = EXCLUDED.{column}" for column in update_columns
    )
    return (
        f"INSERT INTO {table_name} ({insert_columns}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_target}) DO UPDATE SET {update_clause}"
    )


def postgres_upsert_sink(table_name, columns, conflict_columns, row_type, update_columns=None):
    return jdbc_sink(
        make_postgres_upsert(table_name, columns, conflict_columns, update_columns),
        row_type,
    )


def write_upsert(stream, sink_name, table_name, columns, conflict_columns, row_type):
    stream.add_sink(
        postgres_upsert_sink(table_name, columns, conflict_columns, row_type)
    ).name(f"postgres-{sink_name}")


def add_optional_jars(env):
    jars_dir = os.environ.get("FLINK_EXTRA_JARS_DIR", "/opt/flink/lib")
    jar_names = (
        "flink-sql-connector-kafka-3.1.0-1.18.jar",
        "flink-connector-kafka-3.1.0-1.18.jar",
        "flink-connector-jdbc-3.1.2-1.18.jar",
        "postgresql-42.7.3.jar",
    )

    for jar_name in jar_names:
        jar_path = os.path.join(jars_dir, jar_name)
        if os.path.exists(jar_path):
            env.add_jars(f"file://{jar_path}")


def build_kafka_source():
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.environ.get("FLINK_PARALLELISM", "1")))
    env.enable_checkpointing(5000)
    add_optional_jars(env)

    events = (
        env.from_source(build_kafka_source(), WatermarkStrategy.no_watermarks(), "sales-json-kafka")
        .map(parse_event)
        .filter(has_required_ids)
    )

    customer_type = Types.ROW(
        [
            Types.INT(),
            Types.STRING(),
            Types.STRING(),
            Types.INT(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
        ]
    )
    seller_type = Types.ROW(
        [
            Types.INT(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
        ]
    )
    product_type = Types.ROW(
        [
            Types.INT(),
            Types.STRING(),
            Types.STRING(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.DOUBLE(),
            Types.INT(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
        ]
    )
    store_type = Types.ROW(
        [
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
        ]
    )
    supplier_type = Types.ROW(
        [
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
        ]
    )
    fact_type = Types.ROW(
        [
            Types.INT(),
            Types.STRING(),
            Types.INT(),
            Types.INT(),
            Types.INT(),
            Types.INT(),
            Types.DOUBLE(),
        ]
    )

    customer_stream = deduplicate(
        events.map(to_customer_row, output_type=customer_type).name("to-dim-customer"),
        lambda row: row[0],
        Types.INT(),
        customer_type,
        "deduplicate-dim-customer",
    )
    seller_stream = deduplicate(
        events.map(to_seller_row, output_type=seller_type).name("to-dim-seller"),
        lambda row: row[0],
        Types.INT(),
        seller_type,
        "deduplicate-dim-seller",
    )
    product_stream = deduplicate(
        events.map(to_product_row, output_type=product_type).name("to-dim-product"),
        lambda row: row[0],
        Types.INT(),
        product_type,
        "deduplicate-dim-product",
    )
    store_stream = deduplicate(
        events.map(to_store_row, output_type=store_type).name("to-dim-store"),
        lambda row: row_key(row[0], row[2], row[4]),
        Types.STRING(),
        store_type,
        "deduplicate-dim-store",
    )
    supplier_stream = deduplicate(
        events.map(to_supplier_row, output_type=supplier_type).name("to-dim-supplier"),
        lambda row: row_key(row[0], row[2]),
        Types.STRING(),
        supplier_type,
        "deduplicate-dim-supplier",
    )
    fact_stream = events.map(to_fact_row, output_type=fact_type).name("to-fact-sales")

    write_upsert(
        customer_stream,
        "dim-customer",
        "dim_customer",
        (
            "customer_id",
            "first_name",
            "last_name",
            "age",
            "email",
            "country",
            "postal_code",
            "pet_type",
            "pet_name",
            "pet_breed",
        ),
        ("customer_id",),
        customer_type,
    )
    write_upsert(
        seller_stream,
        "dim-seller",
        "dim_seller",
        ("seller_id", "first_name", "last_name", "email", "country", "postal_code"),
        ("seller_id",),
        seller_type,
    )
    write_upsert(
        product_stream,
        "dim-product",
        "dim_product",
        (
            "product_id",
            "name",
            "category",
            "price",
            "weight",
            "color",
            "size",
            "brand",
            "material",
            "description",
            "rating",
            "reviews",
            "release_date",
            "expiry_date",
            "pet_category",
        ),
        ("product_id",),
        product_type,
    )
    write_upsert(
        store_stream,
        "dim-store",
        "dim_store",
        ("name", "location", "city", "state", "country", "phone", "email"),
        ("name", "city", "country"),
        store_type,
    )
    write_upsert(
        supplier_stream,
        "dim-supplier",
        "dim_supplier",
        ("name", "contact", "email", "phone", "address", "city", "country"),
        ("name", "email"),
        supplier_type,
    )
    write_upsert(
        fact_stream,
        "fact-sales",
        "fact_sales",
        ("sale_id", "sale_date", "customer_id", "seller_id", "product_id", "quantity", "total_price"),
        ("sale_id",),
        fact_type,
    )

    env.execute("sales-to-star-datastream")


if __name__ == "__main__":
    main()
