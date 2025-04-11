{% snapshot estructura_organica %}

{% set new_schema = target.schema + '_snapshot' %}

{{
    config(
      target_database='junta_andalucia',
      target_schema=new_schema,
      unique_key=['centro_gestor'],
      strategy = 'timestamp',
      updated_at='update_at'
    )
}}

select * from  {{ ref('dim_estr_organica')}}


{% endsnapshot %}