{% snapshot fk_syfo_person_oversikt_status__snapshot%}

{{
    config(
      target_schema='dvh_syfo',
      unique_key='uuid',
      strategy='check',
      check_cols=['tildelt_enhet'],
      invalidate_hard_deletes=True
    )
}}

select * from {{ source('modia', 'fk_syfo_person_oversikt_status') }}
where tildelt_enhet  != 'None'


{% endsnapshot %}
