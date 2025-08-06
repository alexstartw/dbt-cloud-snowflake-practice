-- models/marts/dim/dim_menu_item.sql
SELECT DISTINCT
  menu_item_id,
  menu_item_name,
  item_category,
  item_subcategory,
  truck_brand_name,
  menu_type
FROM {{ ref('stg_menu') }}
