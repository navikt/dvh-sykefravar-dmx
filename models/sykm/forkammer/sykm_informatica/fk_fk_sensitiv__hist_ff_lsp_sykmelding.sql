WITH source AS (
  SELECT * FROM {{ source('fk_sensitiv', 'hist_ff_lsp_sykmelding') }}
)

SELECT * FROM source
