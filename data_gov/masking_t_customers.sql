ALTER TABLE processing_catalog.schema_harmonized_dimension.t_customers 
ALTER COLUMN phone_number SET MASK processing_catalog.schema_harmonized_dimension.masking_phone;

ALTER TABLE processing_catalog.schema_harmonized_dimension.t_customers 
ALTER COLUMN email SET MASK processing_catalog.schema_harmonized_dimension.masking_email;

ALTER TABLE processing_catalog.schema_harmonized_dimension.t_customers 
ALTER COLUMN address SET MASK processing_catalog.schema_harmonized_dimension.masking_address;
