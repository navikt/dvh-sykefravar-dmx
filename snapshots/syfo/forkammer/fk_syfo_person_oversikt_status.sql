{% snapshot fk_syfo_person_oversikt_status__snapshot_P%}

{{
    config(
      target_schema='dvh_syfo',
      unique_key='id',
      strategy='check',
      check_cols=['fk_person1','tildelt_enhet','tildelt_enhet_updated_at'],
      invalidate_hard_deletes=True
    )
}}

SELECT
   TO_CHAR(fk_person1) || '-' || TO_CHAR(tildelt_enhet) || '-'  ||  TO_CHAR(tildelt_enhet_updated_at)   AS id
  ,oversikt_status.*
FROM
 {{ source('dmx_pox_dialogmote', 'fk_syfo_person_oversikt_status') }} oversikt_status
 where tildelt_enhet  != 'None'




{% endsnapshot %}
