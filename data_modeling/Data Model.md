# Data Model Overview

## 1. Dimension Tables

### `dim_stores`
Contains store-related attributes:
- `store_id` (Primary Key)
- `store_name`
- `location`
- `manager_id`
- `opening_date`

### `dim_customers`
Contains customer-related attributes:
- `customer_id` (Primary Key)
- `first_name`
- `last_name`
- `email`
- `phone_number`
- `loyalty_points`
- `membership_status`
- `address`

### `dim_products`
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

### `sales_transactions`
Captures high-level transactional data:
- `invoice_id` (Primary Key)
- `store_id` (Foreign Key)
- `customer_id` (Foreign Key)
- `transaction_date_time`
- `payment_type`
- `total_amount`

### `sales_line_items`
Provides line-item details for transactions:
- `line_item_id` (Primary Key)
- `invoice_id` (Foreign Key)
- `product_id` (Foreign Key)
- `quantity`
- `unit_price`
- `total_price`
- `discount_applied`

### `sales_facts`
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
  - `dim_stores`
  - `dim_customers`
  - `dim_products`

## 4. Data Relationships
- `sales_facts` links to:
  - `dim_stores` via `store_id`
  - `dim_customers` via `customer_id`
  - `dim_products` via `product_id`
