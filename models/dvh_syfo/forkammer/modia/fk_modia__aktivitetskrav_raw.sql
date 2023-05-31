
WITH aktivitetskrav_raw AS (
  SELECT * FROM {{ source('modia', 'raw_aktivitetskrav_patch') }}
)
select * from aktivitetskrav_raw
