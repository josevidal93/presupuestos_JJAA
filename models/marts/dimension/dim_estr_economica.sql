
{{ config(materialized = 'incremental',  incremental_strategy = 'merge' , unique_key = ['subconcepto', 'gasto_ingreso','update_at'])}}


with legacy as
(
    select * from {{ ref("stg_legacy_estructura_economica")}}
)
, capitulo as (
    select CAPITULO, GASTO_INGRESO
    ,"DESCRIPCION CORTA" DESCRIPCION_CORTA_CAPITULO
    , "DESCRIPCION LARGA" DESCRIPCION_LARGA_CAPITULO
    , UPDATE_AT
    from legacy 
    where
        capitulo is not null 
        and ARTICULO is null
        and CONCEPTO is null
        and SUBCONCEPTO is null
)
, ARTICULO as (
    select ARTICULO, GASTO_INGRESO
    ,"DESCRIPCION CORTA" DESCRIPCION_CORTA_ARTICULO
    , "DESCRIPCION LARGA" DESCRIPCION_LARGA_ARTICULO
    , UPDATE_AT
    from legacy 
    where
        capitulo is not null 
        and ARTICULO is not null
        and CONCEPTO is null
        and SUBCONCEPTO is null
)
, CONCEPTO as (
        select CONCEPTO, GASTO_INGRESO
    ,"DESCRIPCION CORTA" DESCRIPCION_CORTA_CONCEPTO
    , "DESCRIPCION LARGA" DESCRIPCION_LARGA_CONCEPTO
    , UPDATE_AT
    from legacy 
    where
        capitulo is not null 
        and ARTICULO is not null
        and CONCEPTO is not null
        and SUBCONCEPTO is null
)
, SUBCONCEPTO as (
        select * 
    from legacy 
    where
        capitulo is not null 
        and ARTICULO is not null
        and CONCEPTO is not null
        and SUBCONCEPTO is not null
)


select  SUBCONCEPTO.GASTO_INGRESO
,SUBCONCEPTO.CAPITULO
,DESCRIPCION_CORTA_CAPITULO
,DESCRIPCION_LARGA_CAPITULO
,SUBCONCEPTO.ARTICULO
,DESCRIPCION_CORTA_ARTICULO
,DESCRIPCION_LARGA_ARTICULO
,SUBCONCEPTO.CONCEPTO
,DESCRIPCION_CORTA_CONCEPTO
,DESCRIPCION_LARGA_CONCEPTO
,SUBCONCEPTO.SUBCONCEPTO
,"DESCRIPCION CORTA" DESCRIPCION_CORTA_SUBCONCEPTO
, "DESCRIPCION LARGA" DESCRIPCION_LARGA_SUBCONCEPTO
, GREATEST(SUBCONCEPTO.UPDATE_AT,CONCEPTO.UPDATE_AT, ARTICULO.UPDATE_AT, CAPITULO.update_at) update_at    
 from SUBCONCEPTO
inner join CONCEPTO on CONCEPTO.CONCEPTO = SUBCONCEPTO.CONCEPTO and CONCEPTO.GASTO_INGRESO = SUBCONCEPTO.GASTO_INGRESO
inner join ARTICULO on ARTICULO.ARTICULO = SUBCONCEPTO.ARTICULO and ARTICULO.GASTO_INGRESO = SUBCONCEPTO.GASTO_INGRESO
inner join CAPITULO on CAPITULO.CAPITULO = SUBCONCEPTO.CAPITULO and CAPITULO.GASTO_INGRESO = SUBCONCEPTO.GASTO_INGRESO
{% if is_incremental() %}
    where GREATEST(SUBCONCEPTO.UPDATE_AT,CONCEPTO.UPDATE_AT, ARTICULO.UPDATE_AT, CAPITULO.update_at) > ( Select max(update_at) from {{ this }})
  {% endif %}
