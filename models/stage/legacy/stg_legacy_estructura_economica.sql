with source as (
        select * from {{ source('legacy', 'estructura_economica') }}
        {{ param_execution() }}
  ),
  renamed as (
      select
          {{ adapter.quote("EJERCICIO") }},
        {{ adapter.quote("GASTO/INGRESO") }} as gasto_ingreso,
        {{ adapter.quote("CAPITULO") }},
        {{ adapter.quote("ARTICULO") }},
        {{ adapter.quote("CONCEPTO") }},
        {{ adapter.quote("SUBCONCEPTO") }},
        {{ adapter.quote("DESCRIPCION CORTA") }},
        {{ adapter.quote("DESCRIPCION LARGA") }},
        DATEFROMPARTS({{ adapter.quote("EJERCICIO") }}, 1, 1) AS update_at


      from source
  )
  select * from renamed
    