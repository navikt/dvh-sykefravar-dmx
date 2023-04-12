{% snapshot fk_syfo_person_oversikt_status__snapshot_v2%}

{{
    config(
      target_schema='dvh_syfo',
      unique_key='uuid',
      strategy='check',
      check_cols=['tildelt_enhet'],
      invalidate_hard_deletes=True
    )
}}

SELECT
  oversikt_status.*
FROM
  {{ ref ('fk_modia__oversikt_person_status') }} oversikt_status



{% endsnapshot %}
