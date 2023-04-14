{% snapshot fk_syfo_person_oversikt_status__snapshot_t2%}

{{
    config(
      target_schema='dvh_syfo',
      unique_key='uuid',
      strategy='timestamp',
      updated_at='kilde_sist_endret_dato',
      invalidate_hard_deletes=True
    )
}}

-- dette er endring

SELECT
  oversikt_status.*
FROM
  {{ ref ('fk_modia__oversikt_person_status') }} oversikt_status



{% endsnapshot %}
