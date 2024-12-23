CREATE or REPLACE VIEW processing_catalog.curated_layer.v_customer_summary
AS
SELECT
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    SUM(s.quantity * s.unit_price) AS Total_Revenue,
    COUNT(s.invoice_id) AS Number_of_Purchases,
    AVG(s.quantity * s.unit_price) AS Average_Order_Value,
    (AVG(s.quantity * s.unit_price) * COUNT(s.invoice_id)) AS Customer_Lifetime_Value,
    MAX(c.loyalty_points) AS Loyalty_Points,
    MAX(c.membership_status) AS Membership_Status
FROM
    processing_catalog.schema_harmonized_facts.t_sales_facts s
JOIN
    processing_catalog.schema_harmonized_dimension.t_customers c
ON
    s.customer_id = c.customer_id
GROUP BY
    CONCAT(c.first_name, ' ', c.last_name)
ORDER BY
    Total_Revenue DESC;
