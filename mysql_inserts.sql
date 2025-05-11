-- Insert Carriers
INSERT INTO carriers (name, contact_email, phone, tracking_url_template)
VALUES
  ('FedEx', 'support@fedex.com', '800-463-3339', 'https://www.fedex.com/fedextrack/?tracknumbers={tracking_number}'),
  ('UPS', 'customer.service@ups.com', '800-742-5877', 'https://www.ups.com/track?tracknum={tracking_number}'),
  ('DHL', 'contact@dhl.com', '800-225-5345', 'https://www.dhl.com/en/express/tracking.html?AWB={tracking_number}');

-- Insert Shipping Services
INSERT INTO shipping_services (carrier_id, service_name, estimated_days, cost_estimate)
VALUES
  (1, 'FedEx Ground', 5, 12.99),
  (1, 'FedEx Express', 2, 29.99),
  (2, 'UPS Standard', 5, 10.99),
  (2, 'UPS Express', 1, 24.99),
  (3, 'DHL Standard', 7, 15.99);

-- Insert Shipments
INSERT INTO shipments (order_id, carrier_id, service_id, tracking_number, shipping_status, shipped_date, expected_delivery_date, shipping_cost)
VALUES
  (101, 1, 1, '1234FEDX', 'created', '2025-05-01', '2025-05-06', 12.99),
  (102, 2, 2, '5678UPS', 'in_transit', '2025-05-02', '2025-05-03', 24.99),
  (103, 3, 5, '9101DHL', 'delivered', '2025-05-03', '2025-05-10', 15.99);

-- Insert Shipment Events
INSERT INTO shipment_events (shipment_id, status, location, event_time, notes)
VALUES
  (1, 'created', 'New York, NY', '2025-05-01 10:00:00', 'Shipment created and awaiting pickup'),
  (2, 'in_transit', 'Chicago, IL', '2025-05-02 14:00:00', 'Shipment en route to destination'),
  (3, 'delivered', 'Madison, WI', '2025-05-03 11:00:00', 'Delivered to customer at 11:00 AM'),
  (1, 'in_transit', 'Philadelphia, PA', '2025-05-02 16:00:00', 'Shipment in transit, estimated delivery 5/6');

-- Update Shipment Status to Delivered
UPDATE shipments
SET shipping_status = 'delivered', actual_delivery_date = '2025-05-03'
WHERE shipment_id = 3;

-- Update Tracking Number for Shipment
UPDATE shipments
SET tracking_number = '4321FEDX'
WHERE shipment_id = 1;

-- Update Shipping Cost
UPDATE shipments
SET shipping_cost = 27.99
WHERE shipment_id = 2;

-- Update Shipment Event (add more event notes)
UPDATE shipment_events
SET notes = 'Shipment delayed due to weather, new expected delivery is 5/7.'
WHERE event_id = 4;

-- Delete a Shipment Event
DELETE FROM shipment_events
WHERE event_id = 2;

-- Delete a Shipment (also deletes associated events due to foreign key constraint)
DELETE FROM shipments
WHERE shipment_id = 3;

-- Delete a Carrier (also deletes associated shipping services if cascading delete is configured)
DELETE FROM carriers
WHERE carrier_id = 2;

-- Delete a Shipping Service (e.g., UPS Standard)
DELETE FROM shipping_services
WHERE service_id = 3;

-- Bulk Update Shipment Status to Delivered
UPDATE shipments
SET shipping_status = 'delivered', actual_delivery_date = CURDATE()
WHERE expected_delivery_date <= CURDATE() AND shipping_status != 'delivered';

-- Delete Shipments Older Than 30 Days
DELETE FROM shipments
WHERE shipped_date < CURDATE() - INTERVAL 30 DAY;

-- Adding a New Shipment Event for Tracking
INSERT INTO shipment_events (shipment_id, status, location, event_time, notes)
VALUES
  (1, 'delayed', 'Cleveland, OH', '2025-05-03 12:00:00', 'Delayed due to weather, new expected delivery date 5/7');


kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic  \
  --from-beginning