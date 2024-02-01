WITH motebehov AS (
  SELECT * FROM {{ source('modia', 'fk_motebehov_sky') }}
)
select * from motebehov