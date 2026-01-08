WITH customer_metrics AS (
    SELECT
        f.customer_key,
        MAX(f.order_timestamp) AS last_order_date,
        DATEDIFF(day, MAX(f.order_timestamp), CURRENT_DATE()) AS days_since_last_order,
        COUNT(DISTINCT f.order_key) AS total_orders,
        SUM(f.line_revenue) AS lifetime_value
    FROM {{ ref('fact_orders') }} f
    GROUP BY f.customer_key
)
SELECT *
FROM customer_metrics   -- <--- PAS DE POINT-VIRGULE ICI
