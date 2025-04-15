{{ config(materialized = 'incremental',  incremental_strategy = 'append') }}

with source as (
        select * from {{ ref('estructura_funcional') }} 
  )
   
  select GRUPO
  ,FUNCION
  ,SUBFUNCION
  , DESCRIPCION_CORTA_GRUPO
  , DESCRIPCION_LARGA_GRUPO
  , DESCRIPCION_CORTA_FUNCION
  , DESCRIPCION_LARGA_FUNCION
  , DESCRIPCION_CORTA_SUBFUNCION
  , DESCRIPCION_LARGA_SUBFUNCION
  , dbt_updated_at UPDATE_AT
  , dbt_valid_from valid_from
  , dbt_valid_to valid_to
  FROM source


    