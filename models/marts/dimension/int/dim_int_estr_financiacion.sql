 

with source as (
        select * from {{ ref('stg_legacy_estructura_financiacion') }} 
  ),
  renamed as (
      select
          "GASTO/INGRESO" AS GASTO_INGRESO
          , ORIGEN
          , FONDO
          , FINANCIACION 
          , "DESCRIPCION CORTA" AS DESCRIPCION_CORTA_FINANCIACION
          , "DESCRIPCION LARGA" AS DESCRIPCION_LARGA_FINANCIACION
          , UPDATE_AT
      from source
  )
  select * from renamed
 