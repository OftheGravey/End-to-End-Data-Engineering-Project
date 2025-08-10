## Flink Extractor
Contains Flink jobs used for extracting data from the CDC pipeline.

Jobs have been divided into three groups, landing, dimensions and facts. \
The dimension and fact jobs generate one table/topic for their associated dimension/fact. The fact tables are dependent on a subset of dimension jobs so the synthetic IDs can be passed forward to facts, allowing SCD2 to be supported. \
The landing jobs are for generating a bronze/staging layer between the Flink processing jobs and what Debezium produces in the Kafka cluster. This is done because separating these topics it gives us more control over reprocessing. All tracked tables in the source databases have a landing job.


## Dimension Jobs
BooksDimensionJob 
* Responsible for d_books
* Sourced from books and authors in store_db
CarrierServiceDimensionJob 
* Responsible for d_carrier_services
* Sourced from carrier_services and carriers in shipping_db
CustomersDimensionJob 
* Responsible for d_customers
* Sourced from customers in store_db
OrdersDimensionJob
* Responsible for d_orders
* Sourced form orders in store_db

## Fact Jobs
OrderItemsFactJob
* Responsible for f_orders_items
* Sourced from order_items in store_db
* Dimensions included: d_customers, d_orders, d_books
ShipmentEventsFactJob
* Responsible for f_shipment_events
* Sourced from shipment_events and shipments in shipping_db
* Dimensions included: d_orders, d_carrier_services