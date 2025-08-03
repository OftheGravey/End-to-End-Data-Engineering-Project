import dagster as dg
import requests
import logging
import time
import json

FLINK_BASE_URL = "http://jobmanager:8081"
JAR_ID = "flink-extractor-1.0-SNAPSHOT.jar"


class FlinkJobConfig(dg.Config):
    class_name: str


def pull_most_recent(jar_id):
    url = f"{FLINK_BASE_URL}/jars/"

    response = requests.get(url)
    response.raise_for_status()

    files = response.json()["files"]
    files = [x for x in files if x["name"] == jar_id]
    most_recent_jar = max(files, key=lambda x: x["uploaded"])

    return most_recent_jar["id"]


@dg.op()
def monitor_flink_job(context: dg.OpExecutionContext, flink_job_id: str):
    try:
        url = f"{FLINK_BASE_URL}/jobs/{flink_job_id}"
        while 1:
            response = requests.get(url)
            response.raise_for_status()
            response_json = response.json()

            print("Job State", response_json["state"])
            print("Job Duration", response_json["duration"])

            if response_json["state"] != "RUNNING":
                raise dg.Failure()

            time.sleep(5)
    except dg.DagsterExecutionInterruptedError:
        # Close resources
        url = f"{FLINK_BASE_URL}/jobs/{flink_job_id}/stop"
        response = requests.post(url)
        response.raise_for_status()

        print("Stream closed")
    except Exception as e:
        raise e


@dg.op(out={"job_id": dg.Out()})
def trigger_flink_job(
    context: dg.OpExecutionContext,
    config: FlinkJobConfig,
    upstream_flink_job_id: list[str] = None,
) -> str:
    most_recent_jar = pull_most_recent(JAR_ID)
    url = f"{FLINK_BASE_URL}/jars/{most_recent_jar}/run"
    print("JAR URL:", url)
    response = requests.post(url, params={"entry-class": config.class_name})
    response.raise_for_status()
    job_id = response.json()["jobid"]

    # Introduce delay to allow Kafka topics to be
    #  added to before upstream jobs start
    time.sleep(10)

    return job_id


@dg.job(
    config=dg.RunConfig(
        ops={
            "Orders_Landing": FlinkJobConfig(
                class_name="com.extractor.flink.jobs.landing.OrdersLandingJob",
            ),
            "Authors_Landing": FlinkJobConfig(
                class_name="com.extractor.flink.jobs.landing.AuthorsLandingJob",
            ),
            "Customers_Landing": FlinkJobConfig(
                class_name="com.extractor.flink.jobs.landing.CustomersLandingJob",
            ),
            "Books_Landing": FlinkJobConfig(
                class_name="com.extractor.flink.jobs.landing.BooksLandingJob",
            ),
            "Order_Items_Landing": FlinkJobConfig(
                class_name="com.extractor.flink.jobs.landing.OrderItemsLandingJob",
            ),
            "Carriers_Landing": FlinkJobConfig(
                class_name="com.extractor.flink.jobs.landing.CarriersLandingJob",
            ),
            "Shipping_Services_Landing": FlinkJobConfig(
                class_name="com.extractor.flink.jobs.landing.ShippingServicesLandingJob",
            ),
            "Shipments_Landing": FlinkJobConfig(
                class_name="com.extractor.flink.jobs.landing.ShipmentsLandingJob",
            ),
            "Shipment_Events_Landing": FlinkJobConfig(
                class_name="com.extractor.flink.jobs.landing.ShipmentEventsLandingJob",
            ),
            "Orders_Dimension": FlinkJobConfig(
                class_name="com.extractor.flink.jobs.dimensions.OrdersDimensionJob",
            ),
            "Customers_Dimension": FlinkJobConfig(
                class_name="com.extractor.flink.jobs.dimensions.CustomersDimensionJob",
            ),
            "Books_Dimension": FlinkJobConfig(
                class_name="com.extractor.flink.jobs.dimensions.BooksDimensionJob",
            ),
            "Carrier_Services_Dimension": FlinkJobConfig(
                class_name="com.extractor.flink.jobs.dimensions.CarrierServiceDimensionJob",
            ),
            "Shipment_Event_Facts": FlinkJobConfig(
                class_name="com.extractor.flink.jobs.facts.ShipmentEventsFactJob"
            ),
            "Order_Item_Facts": FlinkJobConfig(
                class_name="com.extractor.flink.jobs.facts.OrderItemsFactJob",
            ),
        }
    )
)
def streaming_pipeline():
    # Orders
    orders_landing_job_id = trigger_flink_job.alias("Orders_Landing")()
    monitor_flink_job.alias("Orders_Landing_Monitor")(orders_landing_job_id)
    orders_dimension_job_id = trigger_flink_job.alias("Orders_Dimension")(
        upstream_flink_job_id=[orders_landing_job_id]
    )
    monitor_flink_job.alias("Orders_Dimensions_Monitor")(orders_dimension_job_id)

    # Customers
    customers_landing_job_id = trigger_flink_job.alias("Customers_Landing")()
    monitor_flink_job.alias("Customers_Landing_Monitor")(customers_landing_job_id)
    customers_dimension_job_id = trigger_flink_job.alias("Customers_Dimension")(
        upstream_flink_job_id=[customers_landing_job_id]
    )
    monitor_flink_job.alias("Customers_Dimensions_Monitor")(customers_dimension_job_id)

    # Books & Authors
    books_landing_job_id = trigger_flink_job.alias("Books_Landing")()
    monitor_flink_job.alias("Books_Landing_Monitor")(books_landing_job_id)
    authors_landing_job_id = trigger_flink_job.alias("Authors_Landing")()
    monitor_flink_job.alias("Authors_Landing_Monitor")(authors_landing_job_id)
    books_dimension_job_id = trigger_flink_job.alias("Books_Dimension")(
        upstream_flink_job_id=[authors_landing_job_id, books_landing_job_id]
    )
    monitor_flink_job.alias("Books_Dimension_Monitoring")(books_dimension_job_id)

    # Carrier Services
    carriers_landing_job_id = trigger_flink_job.alias("Carriers_Landing")()
    monitor_flink_job.alias("Carriers_Landing_Monitor")(carriers_landing_job_id)
    shipping_services_landing_job_id = trigger_flink_job.alias(
        "Shipping_Services_Landing"
    )()
    monitor_flink_job.alias("Shipping_Services_Landing_Monitor")(
        shipping_services_landing_job_id
    )
    carrier_services_dimension_job_id = trigger_flink_job.alias(
        "Carrier_Services_Dimension"
    )(
        upstream_flink_job_id=[
            carriers_landing_job_id,
            shipping_services_landing_job_id,
        ],
    )
    monitor_flink_job.alias("Carrier_Services_Dimension_Monitor")(
        carrier_services_dimension_job_id
    )

    # order item facts
    order_items_landing_job_id = trigger_flink_job.alias("Order_Items_Landing")()
    monitor_flink_job.alias("Order_Items_Landing_Monitor")(order_items_landing_job_id)
    order_items_fact_job_id = trigger_flink_job.alias("Order_Item_Facts")(
        upstream_flink_job_id=[
            order_items_landing_job_id,
            books_dimension_job_id,
            customers_dimension_job_id,
            orders_dimension_job_id,
        ]
    )
    monitor_flink_job.alias("Order_Items_Fact_Monitor")(order_items_fact_job_id)

    # Shipping Events
    shipment_events_landing_job_id = trigger_flink_job.alias(
        "Shipment_Events_Landing"
    )()
    monitor_flink_job.alias("Shipment_Events_Landing_Monitor")(
        shipment_events_landing_job_id
    )
    shipments_landing_job_id = trigger_flink_job.alias("Shipments_Landing")()
    monitor_flink_job.alias("Shipments_Landing_Monitor")(shipments_landing_job_id)
    shipment_events_facts_job_id = trigger_flink_job.alias("Shipment_Event_Facts")(
        upstream_flink_job_id=[
            shipments_landing_job_id,
            shipment_events_landing_job_id,
            orders_dimension_job_id,
            carrier_services_dimension_job_id,
        ],
    )
    monitor_flink_job.alias("Shipment_Event_Facts_Monitor")(
        shipment_events_facts_job_id
    )
