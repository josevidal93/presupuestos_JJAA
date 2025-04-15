{{ config(materialized = 'incremental',  incremental_strategy = 'append')}}


with source as (
        select * from {{ ref('estructura_financiacion') }} 
  )
       select
           GASTO_INGRESO
          , ORIGEN
          , FONDO
          , FINANCIACION 
          , DESCRIPCION_CORTA_FINANCIACION
          , DESCRIPCION_LARGA_FINANCIACION
          , dbt_updated_at UPDATE_AT
          , dbt_valid_from valid_from
          , dbt_valid_to valid_to
      from source
   