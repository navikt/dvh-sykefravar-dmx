WITH source AS (
  SELECT * FROM {{ source('fk_sykm_informatica', 'hist_ff_lsp_sykmelding') }}
)

SELECT * FROM source
