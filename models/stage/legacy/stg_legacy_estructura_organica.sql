{{ config(
    materialized='table'
    ,schema = 'stg'
) }}

with source as (
        select * from {{ source('legacy', 'estructura_organica') }} 
        {{ param_execution() }}
        ),
  renamed as (
      select
          {{ adapter.quote("EJERCICIO") }},
        {{ adapter.quote("CENTRO GESTOR") }} AS CENTRO_GESTOR,
        {{ adapter.quote("DESCRIPCION CORTA") }} AS DESCRIPCION_CORTA,
        {{ adapter.quote("DESCRIPCION LARGA") }} AS DESCRIPCION_LARGA
        , DATEFROMPARTS({{ adapter.quote("EJERCICIO") }}, 1, 1) AS update_at

      from source
  )
  select * from renamed
    