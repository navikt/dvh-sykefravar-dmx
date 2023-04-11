{% snapshot fk_syfo_person_oversikt_status__snapshot_v3%}

{{
    config(
      target_schema='dvh_syfo',
      unique_key='id',
      strategy='check',
      check_cols=['fk_person1','tildelt_enhet','kilde_sist_endret_dato'],
      invalidate_hard_deletes=True
    )
}}

SELECT
   TO_CHAR(fk_person1) || '-' || TO_CHAR(tildelt_enhet) || '-'
     || '-'  ||  TO_CHAR(kilde_sist_endret_dato)  AS id

  ,oversikt_status.*
FROM
  {{ ref ('fk_modia__oversikt_person_status') }} oversikt_status



{% endsnapshot %}
