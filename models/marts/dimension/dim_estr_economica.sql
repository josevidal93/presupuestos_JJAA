
{{ config(    materialized = 'incremental',     incremental_strategy = 'append' ) }}


with snapshot_dim as
(
    select * from {{ ref("estructura_economica")}}
)

select  GASTO_INGRESO
,CAPITULO
,DESCRIPCION_CORTA_CAPITULO
,DESCRIPCION_LARGA_CAPITULO
,ARTICULO
,DESCRIPCION_CORTA_ARTICULO
,DESCRIPCION_LARGA_ARTICULO
,CONCEPTO
,DESCRIPCION_CORTA_CONCEPTO
,DESCRIPCION_LARGA_CONCEPTO
,SUBCONCEPTO
,DESCRIPCION_CORTA_SUBCONCEPTO
,DESCRIPCION_LARGA_SUBCONCEPTO
,dbt_updated_at update_at   
, dbt_valid_from valid_from
, dbt_valid_to valid_to
 from snapshot_dim
 