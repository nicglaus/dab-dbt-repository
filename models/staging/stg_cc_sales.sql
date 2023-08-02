{{ config(
    materialized='table',
    partition_by={
      "field": "date_date",
      "data_type": "date",
      "granularity": "day"
    }
)}}

SELECT
  date_date
  ### Key ###  
  ,product_id
  ###########
  ,quantity AS qty
FROM `raw_data_circle.raw_cc_sales`
