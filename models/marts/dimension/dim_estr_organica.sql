{{ config(materialized = 'incremental',  incremental_strategy = 'append') }}


with source as 
(
    select * from {{ ref('estructura_organica')}} 

)


select CENTRO_GESTOR
, DESCRIPCION_CORTA_CENTRO_GESTOR
, DESCRIPCION_LARGA_CENTRO_GESTOR
, DESCRIPCION_CORTA_L1
, DESCRIPCION_LARGA_L1
, DESCRIPCION_CORTA_L2
, DESCRIPCION_LARGA_L2
, DESCRIPCION_CORTA_L3
, DESCRIPCION_LARGA_L3
, dbt_updated_at UPDATE_AT
, dbt_valid_from valid_from
, dbt_valid_to valid_to
from source




 