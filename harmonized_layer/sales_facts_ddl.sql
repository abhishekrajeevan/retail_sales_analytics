CREATE TABLE processing_catalog.schema_harmonized_facts.t_sales_facts
(
  invoice_id STRING,
  line_item_id STRING,
  store_id STRING,
  customer_id STRING,
  employee_id STRING,
  product_id STRING,
  payment_type STRING,
  transaction_date_time TIMESTAMP,
  transaction_month DATE,
  quantity INT,
  unit_price DECIMAL(10,2),
  total_price DECIMAL(10,2),
  discount_applied DECIMAL(10,2),
  total_amount DECIMAL(10,2),
  CONSTRAINT pk_line_item_id PRIMARY KEY (line_item_id)
)
USING DELTA
PARTITIONED BY (transaction_month)
COMMENT 'Harmonized Facts table for sales';