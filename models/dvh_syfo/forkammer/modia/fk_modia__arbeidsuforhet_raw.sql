
WITH uforhet_raw AS (

    SELECT * FROM {{ source('modia', 'raw_arbeidsuforhet') }}
)
select * from uforhet_raw
