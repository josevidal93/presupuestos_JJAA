{{ config(enabled = false) }}



select * from {{ ref('stg_legacy_gastos')}} where IMPORTE <0
