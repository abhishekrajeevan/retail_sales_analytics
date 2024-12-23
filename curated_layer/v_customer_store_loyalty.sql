create or replace view processing_catalog.curated_layer.v_customer_store_loyalty
as

with custome_store_cte as(
  SELECT
        customer_id,
        COUNT(DISTINCT store_id) AS store_count
    FROM
        processing_catalog.schema_harmonized_facts.t_sales_facts
    GROUP BY
        customer_id
)
select 
  CASE
        WHEN store_count = 1 THEN 'Exclusive Shoppers'
        ELSE 'Multi-Store Shoppers'
    END AS shopper_type,
  COUNT(customer_id) AS customer_count
  from custome_store_cte
  GROUP BY shopper_type