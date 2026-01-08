-- models/test_fact_orders.sql
SELECT *
FROM {{ ref('fact_orders') }}
LIMIT 10
