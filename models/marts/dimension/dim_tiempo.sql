{{ config(materialized='table') }}

with fechas as (

    select *
    from ( {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2010-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }} )

),

formateada as (
    select
        date_day as fecha,
        extract(year from date_day) as anio,
        extract(month from date_day) as mes,
        to_char(date_day, 'Month') as nombre_mes,
        extract(day from date_day) as dia,
        extract(quarter from date_day) as trimestre,
        extract(dow from date_day) as dia_semana,
        to_char(date_day, 'Day') as nombre_dia_semana,
        case when extract(dow from date_day) in (0,6) then true else false end as es_fin_de_semana
    from fechas
)

select * from formateada
