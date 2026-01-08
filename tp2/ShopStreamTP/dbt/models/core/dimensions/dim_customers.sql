-- Dimension Clients
-- Fichier : models/core/dimensions/dim_customers.sql

{{
    config(
        materialized='table',
        tags=['core', 'dimension']
    )
}}

WITH users AS (
    SELECT * FROM {{ source('staging', 'stg_users') }}
)

SELECT
    u.id AS customer_key,
    u.name,
    u.email,
    u.created_at AS registration_date,
    CURRENT_TIMESTAMP() AS _dbt_updated_at
FROM users u
