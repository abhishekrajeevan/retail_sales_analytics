# Data Model Overview

## The products has a medallion arhitecture with Raw, Harmonizd and Curated layer. We have a star schema model in Harmonized layer and this will be can be used by different teams to get the cleansed, latest and greatest data. The KPIs and consumption for BI workloads is via Curated layer only

## 1. Dimension Tables

### `t_stores`
Contains store-related attributes:
- `store_id` (Primary Key)
- `store_name`
- `location`
- `manager_id`
- `opening_date`

### `t_customers`(SCD2)
Contains customer-related attributes:
- `customer_id` (Primary Key)
- `first_name`
- `last_name`
- `email`
- `phone_number`
- `loyalty_points`
- `membership_status`
- `address`

### `t_products`
Contains product-related attributes:
- `product_id` (Primary Key)
- `product_name`
- `brand`
- `category`
- `gender`
- `size`
- `color`
- `price`
- `availability`

## 2. Fact Tables

### `t_sales_transactions`
Captures high-level transactional data:
- `invoice_id` (Primary Key)
- `store_id` (Foreign Key)
- `customer_id` (Foreign Key)
- `transaction_date_time`
- `payment_type`
- `total_amount`

### `t_sales_line_items`
Provides line-item details for transactions:
- `line_item_id` (Primary Key)
- `invoice_id` (Foreign Key)
- `product_id` (Foreign Key)
- `quantity`
- `unit_price`
- `total_price`
- `discount_applied`

### `t_sales_facts`
Central fact table combining transactional and line-item data:
- `invoice_id` (Primary Key)
- `line_item_id`
- `store_id` (Foreign Key)
- `customer_id` (Foreign Key)
- `product_id` (Foreign Key)
- `transaction_date_time`
- `transaction_month`
- `quantity`
- `unit_price`
- `total_price`
- `total_amount`

## 3. Star Schema Design
The data model follows a **Star Schema** design:
- **Fact Table**: `sales_facts`
- **Dimension Tables**: 
  - `t_stores`
  - `t_customers`
  - `t_products`

## 4. Data Relationships
- `sales_facts` links to:
  - `t_stores` via `store_id`
  - `t_customers` via `customer_id`
  - `t_products` via `product_id`
