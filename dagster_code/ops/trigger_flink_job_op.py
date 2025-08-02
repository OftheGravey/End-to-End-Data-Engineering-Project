import dagster as dg
import requests
import logging
import time
import json


FLINK_BASE_URL = "http://jobmanager:8081"


class FlinkJobConfig(dg.Config):
    jar_id: str
    class_name: str


def pull_most_recent(jar_id):
    url = f"{FLINK_BASE_URL}/jars/"

    response = requests.get(url)
    response.raise_for_status()

    files = response.json()["files"]
    files = [x for x in files if x["name"] == jar_id]
    most_recent_jar = max(files, key=lambda x: x["uploaded"])

    return most_recent_jar["id"]


@dg.failure_hook()
def flink_job_stop_hook(context: dg.HookContext):
    dependent_job_ids = context.op_exception.metadata["dependent_job_ids"]
    dependent_job_ids_list = json.loads(dependent_job_ids)
    for job_id in dependent_job_ids_list:
        url = f"{FLINK_BASE_URL}/jobs/{job_id}/stop"
        response = requests.post(url)


@dg.op()
def monitor_flink_job(context: dg.OpExecutionContext, flink_job_id: str):
    url = f"{FLINK_BASE_URL}/jobs/{flink_job_id}"
    while 1:
        response = requests.get(url)
        response.raise_for_status()
        response_json = response.json()

        print("Job State", response_json["state"])
        print("Job Duration", response_json["duration"])

        if response_json["state"] != "RUNNING":
            raise dg.Failure(
                metadata={
                    "error-logs": f"http://localhost:8081/#/job/completed/{flink_job_id}"
                }
            )

        time.sleep(5)


@dg.op(out={"job_id": dg.Out()})
def trigger_flink_job(
    context: dg.OpExecutionContext,
    config: FlinkJobConfig,
    upstream_flink_job_id: list[str] = None,
) -> str:
    most_recent_jar = pull_most_recent(config.jar_id)
    url = f"{FLINK_BASE_URL}/jars/{most_recent_jar}/run"
    print("JAR URL:", url)
    response = requests.post(url, params={"entry-class": config.class_name})
    response.raise_for_status()
    job_id = response.json()["jobid"]

    return job_id


@dg.job(
    config=dg.RunConfig(
        ops={
            "Orders_Landing": FlinkJobConfig(
                jar_id="flink-extractor-1.0-SNAPSHOT.jar",
                class_name="com.extractor.flink.jobs.landing.OrdersLandingJob",
            ),
            "Authors_Landing": FlinkJobConfig(
                jar_id="flink-extractor-1.0-SNAPSHOT.jar",
                class_name="com.extractor.flink.jobs.landing.AuthorsLandingJob",
            ),
            "Customers_Landing": FlinkJobConfig(
                jar_id="flink-extractor-1.0-SNAPSHOT.jar",
                class_name="com.extractor.flink.jobs.landing.CustomersLandingJob",
            ),
            "Books_Landing": FlinkJobConfig(
                jar_id="flink-extractor-1.0-SNAPSHOT.jar",
                class_name="com.extractor.flink.jobs.landing.BooksLandingJob",
            ),
            "Order_Items_Landing": FlinkJobConfig(
                jar_id="flink-extractor-1.0-SNAPSHOT.jar",
                class_name="com.extractor.flink.jobs.landing.OrderItemsLandingJob",
            ),
            "Orders_Dimension": FlinkJobConfig(
                jar_id="flink-extractor-1.0-SNAPSHOT.jar",
                class_name="com.extractor.flink.jobs.dimensions.OrdersDimensionJob",
            ),
            "Customers_Dimension": FlinkJobConfig(
                jar_id="flink-extractor-1.0-SNAPSHOT.jar",
                class_name="com.extractor.flink.jobs.dimensions.CustomersDimensionJob",
            ),
            "Books_Dimension": FlinkJobConfig(
                jar_id="flink-extractor-1.0-SNAPSHOT.jar",
                class_name="com.extractor.flink.jobs.dimensions.BooksDimensionJob",
            ),
            "Order_Item_Facts": FlinkJobConfig(
                jar_id="flink-extractor-1.0-SNAPSHOT.jar",
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

    # books & authors
    books_landing_job_id = trigger_flink_job.alias("Books_Landing")()
    monitor_flink_job.alias("Books_Landing_Monitor")(books_landing_job_id)
    authors_landing_job_id = trigger_flink_job.alias("Authors_Landing")()
    monitor_flink_job.alias("Authors_Landing_Monitor")(authors_landing_job_id)
    books_dimension_job_id = trigger_flink_job.alias("Books_Dimension")(
        upstream_flink_job_id=[authors_landing_job_id, books_landing_job_id]
    )
    monitor_flink_job.alias("Books_Dimension_Monitoring")(books_dimension_job_id)

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
