-- models/marts/fct/fct_menu_profit.sql
SELECT
  menu_item_id,
  menu_item_name,
  truck_brand_name,
  sale_price_usd,
  cost_of_goods_usd,
  sale_price_usd - cost_of_goods_usd AS profit_usd
FROM {{ ref('stg_menu') }};
