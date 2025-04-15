with gastos as (
    select organica_id,economica_id,FINANCIACION_id,date_day,sum(IMPORTE) importe from {{ ref('fct_gastos')}}
    group by 1,2,3,4
)
, ingresos as (

    select organica_id,economica_id,FINANCIACION_id,date_day,sum(IMPORTE) importe from {{ ref('fct_ingresos')}}
    group by 1,2,3,4
)

select * from gastos 
full outer join ingresos on 
 gastos.organica_id = ingresos.organica_id
and gastos.economica_id = ingresos.economica_id
and gastos.FINANCIACION_id = ingresos.FINANCIACION_id
and gastos.date_day = ingresos.date_day
where (gastos.FINANCIACION_id = '0101'
or ingresos.FINANCIACION_id = '0101')
and (gastos.organica_id = '0100010000'
or ingresos.organica_id = '0100010000')

