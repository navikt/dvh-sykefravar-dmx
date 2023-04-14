{% snapshot fak_dialogmote__snapshot%}

{{
    config(
      target_schema='dvh_syfo',
      unique_key='id',
      strategy='check',
      check_cols='all',
      invalidate_hard_deletes=True
    )
}}

SELECT
  TO_CHAR(fk_person1) || '-' || TO_CHAR(tilfelle_startdato)  AS id
  ,dialogmote.*
FROM
  {{ ref("fak_dialogmote") }} dialogmote

{% endsnapshot %}
