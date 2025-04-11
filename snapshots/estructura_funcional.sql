{% snapshot estructura_funcional %}

{% set new_schema = target.schema + '_snapshot' %}

{{
    config(
      target_database='junta_andalucia',
      target_schema=new_schema,
      unique_key=['SUBFUNCION'],
      strategy = 'timestamp',
      updated_at='update_at'
    )
}}

select * from  {{ ref('dim_estr_funcional')}}


{% endsnapshot %}