-- models/staging/stg_menu.sql
WITH base AS (
  SELECT
    menu_id,
    menu_type_id,
    TRIM(menu_type) AS menu_type,
    TRIM(truck_brand_name) AS truck_brand_name,
    menu_item_id,
    TRIM(menu_item_name) AS menu_item_name,
    item_category,
    item_subcategory,
    cost_of_goods_usd,
    sale_price_usd,
    menu_item_health_metrics_obj
  FROM {{ source('SNOWFLAKE_LEARNING_DB', 'MENU') }}
)

SELECT * FROM base;
