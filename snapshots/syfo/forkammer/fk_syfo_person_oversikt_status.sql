{% snapshot fk_syfo_person_oversikt_status__snapshot%}

{{
    config(
      target_schema='dvh_syfo',
      unique_key='id',
      strategy='check',
      check_cols=['fk_person1','tildelt_enhet'],
      invalidate_hard_deletes=True
    )
}}

SELECT
  TO_CHAR(fk_person1) || '-' || TO_CHAR(tildelt_enhet) AS id
  ,oversikt_status.*
FROM
  {{ ref("fk_person_oversikt_status") }} oversikt_status

{% endsnapshot %}
