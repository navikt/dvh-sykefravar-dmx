WITH source AS (
  SELECT * FROM {{ source('dk_sykm_informatica', 'syk_sykmelding') }}
)

SELECT * FROM source