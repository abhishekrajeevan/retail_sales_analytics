INSERT OVERWRITE TABLE processing_catalog.schema_harmonized_facts.t_sales_facts
SELECT 
  trn.invoice_id,
  line_items.line_item_id,
  trn.store_id,
  trn.customer_id,
  trn.employee_id,
  line_items.product_id,
  trn.payment_type,
  trn.transaction_date_time,
  trn.transaction_month,
  line_items.quantity,
  line_items.unit_price,
  line_items.total_price,
  line_items.discount_applied,
  trn.total_amount
FROM processing_catalog.schema_harmonized_facts.t_sales_line_items_test line_items
INNER JOIN processing_catalog.schema_harmonized_facts.t_sales_transactions_test trn
ON line_items.invoice_id = trn.invoice_id;
