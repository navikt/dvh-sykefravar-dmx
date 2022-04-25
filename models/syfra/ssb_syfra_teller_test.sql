WITH versjon AS (
  SELECT pk_dim_versjon FROM {{ source('dt_p', 'dim_versjon') }}
  WHERE
    tabell_navn = 'SSB_SYFRA_TELLER'
    AND status = 'GODKJENT'
),

final AS (
  SELECT
    fk_person1,
    s_start,
    s_stopp,
    p_start,
    p_slutt,
    gj_uforg,
    kvartal
  FROM {{ source('dt_sensitiv', 'ssb_syfra_teller') }}
  RIGHT JOIN versjon ON fk_dim_versjon = versjon.pk_dim_versjon
)

SELECT * FROM final
