
WITH aktivitetskrav_raw AS (

    SELECT * FROM {{ source('modia', 'raw_aktivitetskrav_dag') }} -- byttest til _patch når overnevnte er i prod
)
select * from aktivitetskrav_raw
