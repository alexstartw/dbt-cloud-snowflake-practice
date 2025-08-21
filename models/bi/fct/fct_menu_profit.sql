-- models/marts/fact/fct_menu_profit.sql
{{ config(
  materialized='incremental',
  unique_key='menu_item_id',
  incremental_strategy='merge',
  on_schema_change='sync_all_columns',
  tags=['mart','fact']
) }}

WITH src AS (
  SELECT
    menu_item_id,
    menu_item_name,
    truck_brand_name,
    sale_price_usd,
    cost_of_goods_usd,
    (sale_price_usd - cost_of_goods_usd) AS profit_usd
  FROM {{ ref('stg_menu') }}
)

SELECT *
FROM src
{% if is_incremental() %}
  -- ── 邊界條件（擇一啟用） ─────────────────────────────────────

  -- 方案 A：以單調遞增 ID 作為界線
  WHERE menu_item_id > (
    SELECT COALESCE(MAX(menu_item_id), 0) FROM {{ this }}
  )

  -- 方案 B：若有 updated_at，建議用時間戳（更穩健）
  -- WHERE updated_at > (
  --   SELECT COALESCE(MAX(updated_at), '1900-01-01'::timestamp) FROM {{ this }}
  -- )

  -- 方案 C：以內容雜湊偵測變更（需在 src 先算 row_hash；此處通常不加 where）
  -- 無 where；交給 MERGE 以 unique_key 做 upsert
{% endif %}
