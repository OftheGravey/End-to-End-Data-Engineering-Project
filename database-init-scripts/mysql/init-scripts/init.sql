-- SCHEMA: shipping_system (MySQL)

-- CARRIERS
CREATE TABLE carriers (
    carrier_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    contact_email VARCHAR(100),
    phone VARCHAR(20),
    tracking_url_template VARCHAR(255), -- e.g., "https://carrier.com/track?num={tracking_number}"
    UNIQUE (name)
);

-- SHIPPING SERVICES
CREATE TABLE shipping_services (
    service_id INT AUTO_INCREMENT PRIMARY KEY,
    carrier_id INT NOT NULL,
    service_name VARCHAR(100) NOT NULL,
    estimated_days INT,
    cost_estimate DECIMAL(10,2),
    FOREIGN KEY (carrier_id) REFERENCES carriers(carrier_id)
);

-- SHIPMENTS
CREATE TABLE shipments (
    shipment_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL, -- corresponds to order_id in PostgreSQL (not FK constraint)
    carrier_id INT NOT NULL,
    service_id INT NOT NULL,
    tracking_number VARCHAR(100) UNIQUE NOT NULL,
    shipping_status ENUM('created', 'in_transit', 'delivered', 'delayed', 'returned', 'cancelled') NOT NULL DEFAULT 'created',
    shipped_date DATE,
    expected_delivery_date DATE,
    actual_delivery_date DATE,
    shipping_cost DECIMAL(10,2),
    FOREIGN KEY (carrier_id) REFERENCES carriers(carrier_id),
    FOREIGN KEY (service_id) REFERENCES shipping_services(service_id)
);

-- SHIPMENT EVENTS (status history)
CREATE TABLE shipment_events (
    event_id INT AUTO_INCREMENT PRIMARY KEY,
    shipment_id INT NOT NULL,
    status ENUM('created', 'in_transit', 'delivered', 'delayed', 'returned', 'cancelled') NOT NULL,
    location VARCHAR(255),
    event_time DATETIME NOT NULL,
    notes TEXT,
    FOREIGN KEY (shipment_id) REFERENCES shipments(shipment_id)
);

GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium';
FLUSH PRIVILEGES;