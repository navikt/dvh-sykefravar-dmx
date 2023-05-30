
WITH aktivitetskrav_raw AS (
  SELECT * FROM {{ source('modia', 'raw_aktivitetskrav_dag') }}
)
select * from aktivitetskrav_raw
