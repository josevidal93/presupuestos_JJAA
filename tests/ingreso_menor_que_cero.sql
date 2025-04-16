{{ config(enabled = false) }}



select * from {{ ref('stg_legacy_ingresos')}} where IMPORTE <0
