with source as (
        select * from {{ source('legacy', 'ingresos') }}
        {{ param_execution() }}

  ),
  renamed as (
      select
          {{ adapter.quote("EJERCICIO") }},
        {{ adapter.quote("CENTRO GESTOR") }} as centro_gestor,
        {{ adapter.quote("ECONOMICA") }},
        {{ adapter.quote("FINANCIACION") }},
        {{ adapter.quote("DESCRIPCION") }}
        ,replace(
          replace({{ adapter.quote("IMPORTE") }}, '.','')
          , ',','.'
        ) importe
        , DATEFROMPARTS({{ adapter.quote("EJERCICIO") }}, 1, 1) AS update_at

      from source
  )
  select * from renamed
    