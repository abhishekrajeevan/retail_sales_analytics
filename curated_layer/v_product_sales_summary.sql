CREATE or REPLACE VIEW processing_catalog.curated_layer.v_product_sales_summary
AS
SELECT
    p.product_name,
    SUM(s.quantity * s.unit_price) AS Total_Sales_Revenue,
    SUM(s.quantity) AS Units_Sold,
    AVG(s.unit_price) AS Average_Selling_Price
FROM
    processing_catalog.schema_harmonized_facts.t_sales_facts s
INNER JOIN
    processing_catalog.schema_harmonized_dimension.t_products p
ON
    s.product_id = p.product_id
GROUP BY
    p.product_name
ORDER BY
    Total_Sales_Revenue DESC;
