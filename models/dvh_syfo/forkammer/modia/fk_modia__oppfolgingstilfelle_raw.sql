
WITH raw_oppfolginstilfelle AS (

    SELECT * FROM {{ source('modia', 'raw_oppfolgingstilfelle') }}
)
select * from raw_oppfolginstilfelle
