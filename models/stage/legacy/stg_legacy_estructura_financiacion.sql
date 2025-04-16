with source as (
        select * from {{ source('legacy', 'estructura_financiacion') }}
        {{ param_execution() }}
  ),
  renamed as (
      select
          *
          , DATEFROMPARTS({{ adapter.quote("EJERCICIO") }}, 1, 1) AS update_at


      from source
  )
  select * from renamed
    