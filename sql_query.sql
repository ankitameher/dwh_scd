1. Create a SQL query to calculate the revenue per customer.
SELECT
    customer_id,
    SUM(total_price) AS total_revenue
FROM
    `testdataproc-382013.dwh_ecom_data.order_fact`
GROUP BY
    customer_id
ORDER BY
    total_revenue DESC;

2. Create a SQL query to calculate the value of goods stored in the different inventory locations.
WITH inventory_value AS (
    SELECT
        i.inventory_location_id,
        i.product_id,
        p.product_price,  -- Assuming 'product_price' is the column for product price in the product table
        i.inventory_quantity,
        i.inventory_quantity * p.product_price AS total_value
    FROM
        `testdataproc-382013.dwh_ecom_data.inventory_fact` AS i
    JOIN
        `testdataproc-382013.dwh_ecom_data.product_dim` AS p
    ON
        i.product_id = p.product_id
)

SELECT
    inventory_location_id,
    SUM(total_value) AS total_inventory_value
FROM
    inventory_value
GROUP BY
    inventory_location_id
ORDER BY
    total_inventory_value DESC;
    
3.Create a SQL query to calculate the average order value per customer.

WITH customer_order_totals AS (
    SELECT
        customer_id,
        SUM(total_price) AS total_order_value,
        COUNT(order_id) AS number_of_orders
    FROM
        `testdataproc-382013.dwh_ecom_data.order_fact`
    GROUP BY
        customer_id
)

SELECT
    customer_id,
    total_order_value / number_of_orders AS average_order_value
FROM
    customer_order_totals
ORDER BY
    average_order_value DESC;


---------------------
WITH customer_order_totals AS (
    SELECT
        o.customer_id,
        SUM(o.total_price) AS total_order_value
    FROM `testdataproc-382013.dwh_ecom_data.order_fact` o
    GROUP BY o.customer_id
)

SELECT
    c.customer_id,
    c.customer_name,
    AVG(cot.total_order_value) AS average_order_value
FROM `testdataproc-382013.dwh_ecom_data.customer_dim` c
LEFT JOIN customer_order_totals cot
ON c.customer_id = cot.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY c.customer_id;
