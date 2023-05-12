
WITH aktivitetskrav_raw AS (
  SELECT * FROM {{ source('modia', 'raw_aktivitetskrav_clob') }}
)
select * from aktivitetskrav_raw
