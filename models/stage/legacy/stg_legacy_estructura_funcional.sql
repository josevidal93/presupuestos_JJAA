{{ config(
    materialized='table'
    ,schema = 'stg'
) }}

with source as (
        select * from {{ source('legacy', 'estructura_funcional') }}
        {{ param_execution() }}

  ),
  renamed as (
      select
          {{ adapter.quote("EJERCICIO") }},
        {{ adapter.quote("GRUPO") }},
        {{ adapter.quote("FUNCION") }},
        {{ adapter.quote("SUBFUNCION") }},
        {{ adapter.quote("PROGRAMA") }},
        {{ adapter.quote("DESCRIPCION CORTA") }},
        {{ adapter.quote("DESCRIPCION LARGA") }}
        , DATEFROMPARTS({{ adapter.quote("EJERCICIO") }}, 1, 1) AS update_at

      from source
  )
  select * from renamed
    