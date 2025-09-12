SELECT
    ddl
FROM
    ecommerce_dw.INFORMATION_SCHEMA.TABLES
WHERE
    table_name = "fact_sales";