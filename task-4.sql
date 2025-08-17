CREATE DATABASE olist_ecommerce;
USE olist_ecommerce;

-- Orders
CREATE TABLE olist_orders (
  order_id VARCHAR(50) PRIMARY KEY,
  customer_id VARCHAR(50),
  order_status VARCHAR(20),
  order_purchase_timestamp DATETIME,
  order_approved_at DATETIME,
  order_delivered_carrier_date DATETIME,
  order_delivered_customer_date DATETIME,
  order_estimated_delivery_date DATETIME
);

-- Customers
CREATE TABLE olist_customers (
  customer_id VARCHAR(50) PRIMARY KEY,
  customer_unique_id VARCHAR(50),
  customer_zip_code_prefix INT,
  customer_city VARCHAR(50),
  customer_state VARCHAR(2)
);

-- Order Items
CREATE TABLE olist_order_items (
  order_id VARCHAR(50),
  order_item_id INT,
  product_id VARCHAR(50),
  seller_id VARCHAR(50),
  shipping_limit_date DATETIME,
  price DECIMAL(10,2),
  freight_value DECIMAL(10,2)
);

-- Payments
CREATE TABLE olist_order_payments (
  order_id VARCHAR(50),
  payment_sequential INT,
  payment_type VARCHAR(20),
  payment_installments INT,
  payment_value DECIMAL(10,2)
);

-- Products
CREATE TABLE olist_products (
  product_id VARCHAR(50),
  product_category_name VARCHAR(50),
  product_name_length INT,
  product_description_length INT,
  product_photos_qty INT,
  product_weight_g INT,
  product_length_cm INT,
  product_height_cm INT,
  product_width_cm INT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_orders_dataset.csv'
INTO TABLE olist_orders
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 ROWS
(
  order_id,
  customer_id,
  order_status,
  @order_purchase_timestamp,
  @order_approved_at,
  @order_delivered_carrier_date,
  @order_delivered_customer_date,
  @order_estimated_delivery_date
)
SET
  order_purchase_timestamp = NULLIF(@order_purchase_timestamp, ''),
  order_approved_at = NULLIF(@order_approved_at, ''),
  order_delivered_carrier_date = NULLIF(@order_delivered_carrier_date, ''),
  order_delivered_customer_date = NULLIF(@order_delivered_customer_date, ''),
  order_estimated_delivery_date = NULLIF(@order_estimated_delivery_date, '');

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_customers_dataset.csv'
INTO TABLE olist_customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_items_dataset.csv'
INTO TABLE olist_order_items
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 ROWS
(
  order_id,
  order_item_id,
  product_id,
  seller_id,
  @shipping_limit_date,
  @price,
  @freight_value
)
SET
  shipping_limit_date = NULLIF(@shipping_limit_date, ''),
  price = NULLIF(@price, ''),
  freight_value = NULLIF(@freight_value, '');

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_payments_dataset.csv'
INTO TABLE olist_order_payments
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 ROWS
(
  order_id,
  payment_sequential,
  payment_type,
  @payment_installments,
  @payment_value
)
SET
  payment_installments = NULLIF(@payment_installments, ''),
  payment_value = NULLIF(@payment_value, '');

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_products_dataset.csv'
INTO TABLE olist_products
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 ROWS
(
  product_id,
  product_category_name,
  @product_name_length,
  @product_description_length,
  @product_photos_qty,
  @product_weight_g,
  @product_length_cm,
  @product_height_cm,
  @product_width_cm
)
SET
  product_name_length = NULLIF(@product_name_length, ''),
  product_description_length = NULLIF(@product_description_length, ''),
  product_photos_qty = NULLIF(@product_photos_qty, ''),
  product_weight_g = NULLIF(@product_weight_g, ''),
  product_length_cm = NULLIF(@product_length_cm, ''),
  product_height_cm = NULLIF(@product_height_cm, ''),
  product_width_cm = NULLIF(@product_width_cm, '');
  
  -- Show first 10 orders with status 'delivered'
SELECT order_id, customer_id, order_status, order_delivered_customer_date
FROM olist_orders
WHERE order_status = 'delivered'
ORDER BY order_delivered_customer_date DESC
LIMIT 10;

-- Number of orders per customer
SELECT customer_id, COUNT(*) AS total_orders
FROM olist_orders
GROUP BY customer_id
ORDER BY total_orders DESC
LIMIT 10;

-- Join orders with customers to get customer city for delivered orders
SELECT o.order_id, o.order_status, c.customer_city, c.customer_state
FROM olist_orders o
INNER JOIN olist_customers c ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
LIMIT 10;

-- Left Join: Customers + Orders (include customers without orders)
SELECT c.customer_id, c.customer_city, COUNT(o.order_id) AS orders_count
FROM olist_customers c
LEFT JOIN olist_orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_city
ORDER BY orders_count DESC
LIMIT 10;

-- Subquery: Customers with More Than 5 Orders
SELECT customer_id
FROM olist_orders
GROUP BY customer_id
HAVING COUNT(*) > 5;

-- Subquery with Join: Details of Customers with More Than 5 Orders
SELECT c.customer_id, c.customer_city, o.total_orders
FROM olist_customers c
JOIN (
  SELECT customer_id, COUNT(*) AS total_orders
  FROM olist_orders
  GROUP BY customer_id
  HAVING total_orders > 5
) o ON c.customer_id = o.customer_id
ORDER BY o.total_orders DESC;

-- Aggregate Functions: Total Revenue and Average Payment per Payment Type
SELECT payment_type,
       SUM(payment_value) AS total_revenue,
       AVG(payment_value) AS avg_payment
FROM olist_order_payments
GROUP BY payment_type
ORDER BY total_revenue DESC;

-- Creating a View for Popular Product Categories
CREATE OR REPLACE VIEW popular_categories AS
SELECT p.product_category_name,
       COUNT(oi.order_id) AS total_orders
FROM olist_order_items oi
JOIN olist_products p ON oi.product_id = p.product_id
GROUP BY p.product_category_name
ORDER BY total_orders DESC;

SELECT * FROM popular_categories LIMIT 10;

-- Top 5 customers by total spending
SELECT c.customer_id, c.customer_city, SUM(p.payment_value) AS total_spent
FROM olist_customers c
JOIN olist_orders o ON c.customer_id = o.customer_id
JOIN olist_order_payments p ON o.order_id = p.order_id
GROUP BY c.customer_id, c.customer_city
ORDER BY total_spent DESC
LIMIT 5;

-- Using Indexes for Optimization
CREATE INDEX idx_orders_customer_id ON olist_orders(customer_id);
CREATE INDEX idx_order_items_product_id ON olist_order_items(product_id);
CREATE INDEX idx_payments_order_id ON olist_order_payments(order_id);




