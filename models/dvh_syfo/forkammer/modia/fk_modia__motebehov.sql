WITH motebehov AS (
  SELECT * FROM {{ source('modia', 'fk_motebehov') }}
)
select * from motebehov