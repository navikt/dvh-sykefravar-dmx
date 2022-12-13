WITH motebehov AS (
  SELECT * FROM {{ source('modia', 'raw_motebehov') }}
)
select * from motebehov