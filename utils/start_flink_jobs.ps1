$FLINK_CONTAINER="end-to-end-jobmanager-1"
$JOB_DIR="app/flink_jobs/jobs"

# List of job file names to run
$jobs = @(
    "books_stream.py",
    "customers_stream.py",
    "order_items_stream.py",
    "orders_stream.py",
    "shipping_services_stream.py",
    "shipments_stream.py",
    "shipment_events_stream.py",
    "carriers_stream.py"
)

# Loop through and run each job in the container
foreach ($job in $jobs) {
    Write-Host "Running Flink job: $job"
    docker exec -it $FLINK_CONTAINER flink run -py "$JOB_DIR/$job"
}