{% snapshot fk_syfo_person_oversikt_status__snapshot_t1%}

{{
    config(
      target_schema='dvh_syfo',
      unique_key='id',
      strategy='timestamp',
      updated_at='kilde_sist_endret_dato',
      invalidate_hard_deletes=True
    )
}}

SELECT
   cast((TO_CHAR(fk_person1) || '-' || TO_CHAR(tildelt_enhet)) as varchar2(35))  AS id

  ,oversikt_status.*
FROM
  {{ ref ('fk_modia__oversikt_person_status') }} oversikt_status



{% endsnapshot %}
