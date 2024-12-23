-- tagging PII data
ALTER TABLE processing_catalog.schema_raw_dimension.t_customers_test 
SET TAGS ('sensitivity' = 'PII', 'data_owner' = 'sales_team');

-- discovering PII data
SELECT * FROM processing_catalog.information_schema.table_tags
WHERE tags['sensitivity'] = 'PII';

-- Read access to Curated layer for DataCosnumers
GRANT SELECT ON VIEW processing_catalog.curated_layer.v_customer_store_loyalty TO `DataConsumers`;
GRANT SELECT ON VIEW processing_catalog.curated_layer.v_customer_summary TO `DataConsumers`;
GRANT SELECT ON VIEW processing_catalog.curated_layer.v_product_sales_summary TO `DataConsumers`;

GRANT USAGE ON SCHEMA processing_catalog.curated_layer TO `DataConsumers`;
