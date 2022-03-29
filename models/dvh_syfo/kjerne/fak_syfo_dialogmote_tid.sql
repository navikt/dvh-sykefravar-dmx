WITH fak_syfo_dialogmote_tid AS (
  SELECT * FROM {{ ref('fak_syfo_dialogmote_org') }}
),

final AS (
  SELECT
    fak_syfo_dialogmote_tid.*,
    to_number(
      to_char(
        fak_syfo_dialogmote_tid.nyeste_tilfelle_startdato, 'YYYYMMDD'
      )
    ) AS fk_dim_tid_tilfelle_startdato
  FROM fak_syfo_dialogmote_tid

)

SELECT * FROM final
