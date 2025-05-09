{{ config(
    materialized='table'
) }}

with stg as (
    select *  from 
    {{ ref('stg_legacy_gastos')}}
)
, dim_estr_economica_snapshot as (
    select * from {{ ref('estructura_economica')}}
    where gasto_ingreso = 'G' and dbt_valid_from = '2015-01-01'
)
, dim_estr_financiacion_snapshot as (
    select * from {{ ref('estructura_financiacion')}}
    where gasto_ingreso = 'G'
)
, dim_estr_funcional_snapshot as
(
    select * from {{ ref('estructura_funcional')}}
)
, dim_estr_organica_snapshot as
(
    select * from {{ ref('estructura_organica')}}
)

select stg.CENTRO_GESTOR as organica_id
       ,stg.funcional as funcional_id
       ,stg.economica as economica_id
       ,stg.FINANCIACION as FINANCIACION_id
       ,stg.update_at as date_day
       ,cast(IMPORTE as double) IMPORTE
       ,current_timestamp() as update_at
       , concat('{{ target.name}}','_user') user_update

from stg
inner join dim_estr_economica_snapshot on dim_estr_economica_snapshot.SUBCONCEPTO = stg.economica 
    and dim_estr_economica_snapshot.dbt_valid_from <= stg.update_at 
    and ((dim_estr_economica_snapshot.dbt_valid_to > stg.update_at OR dim_estr_economica_snapshot.dbt_valid_to IS NULL))
inner join dim_estr_financiacion_snapshot on dim_estr_financiacion_snapshot.FINANCIACION = stg.FINANCIACION 
    and dim_estr_financiacion_snapshot.dbt_valid_from <= stg.update_at 
    and ((dim_estr_financiacion_snapshot.dbt_valid_to > stg.update_at OR dim_estr_financiacion_snapshot.dbt_valid_to IS NULL))
inner join dim_estr_funcional_snapshot on dim_estr_funcional_snapshot.SUBFUNCION = stg.funcional 
    and dim_estr_funcional_snapshot.dbt_valid_from <= stg.update_at 
    and ((dim_estr_funcional_snapshot.dbt_valid_to > stg.update_at OR dim_estr_funcional_snapshot.dbt_valid_to IS NULL))
inner join dim_estr_organica_snapshot on dim_estr_organica_snapshot.CENTRO_GESTOR = stg.CENTRO_GESTOR 
    and dim_estr_organica_snapshot.dbt_valid_from <= stg.update_at 
    and ((dim_estr_organica_snapshot.dbt_valid_to > stg.update_at OR dim_estr_organica_snapshot.dbt_valid_to IS NULL))
