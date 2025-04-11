{% snapshot estructura_financiacion %}

{% set new_schema = target.schema + '_snapshot' %}

{{
    config(
      target_database='junta_andalucia',
      target_schema=new_schema,
      unique_key=['FINANCIACION','GASTO_INGRESO'],

      strategy='timestamp',
      updated_at='update_at'
    )
}}

select * from  {{ ref('dim_estr_financiacion')}}


{% endsnapshot %}