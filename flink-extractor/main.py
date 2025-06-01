from flink_jobs.jobs import (
    order_items_stream,
    orders_stream,
    books_stream,
    authors_stream,
    customers_stream,
    shipment_events_stream,
    shipments_stream,
    shipping_services_stream,
    carriers_stream,
)

def main():
    kafkaGroupName = 'Streaming'

    order_items_stream.order_items_stream(kafkaGroupName)
    orders_stream.orders_stream(kafkaGroupName)
    books_stream.books_stream(kafkaGroupName)
    authors_stream.authors_stream(kafkaGroupName)
    customers_stream.customers_stream(kafkaGroupName)
    shipment_events_stream.shipment_events_stream(kafkaGroupName)
    shipments_stream.shipments_stream(kafkaGroupName)
    shipping_services_stream.shipping_services_stream(kafkaGroupName)
    carriers_stream.carriers_stream(kafkaGroupName)