WITH tilfeller AS (
  SELECT * FROM {{ ref('mk_syfo_tilfeller_join_dialogmote_drp202210') }}
)

, rapportperiode AS (
  SELECT
    '{{ var('rapportperiode') }}' AS periode,
    TO_DATE('{{ var('rapportperiode') }}', 'YYYYMM') AS start_dato,
    LAST_DAY(TO_DATE('{{ var('rapportperiode') }}', 'YYYYMM')) AS slutt_dato
  FROM dual
)

, tilfeller_passerer_26u AS (
  SELECT
    tilfeller.*,
    rapportperiode.periode AS periode,
    rapportperiode.start_dato AS rapportperiode_start_dato,
    rapportperiode.slutt_dato AS rapportperiode_slutt_dato
  FROM tilfeller, rapportperiode
  WHERE tilfeller.tilfelle_startdato
    BETWEEN ADD_MONTHS(rapportperiode.start_dato, - 6)
    AND ADD_MONTHS(rapportperiode.slutt_dato, - 6)
)

, tilfeller_med_flagg1 AS (
  SELECT
    fk_person1,
    tilfelle_startdato,
    kandidatdato,
    DECODE(kandidatdato, NULL, 0, 1) AS kandidat_flagg,
    unntakdato,
    DECODE(unntakdato, NULL, 0, 1) AS unntak_flagg,
    dialogmote_tidspunkt,
  CASE
    WHEN dialogmote_tidspunkt < rapportperiode_start_dato THEN 1 -- TODO
    ELSE 0
  END AS DIALOGMOTE_TIDLIGERE_PERIODE_FLAGG,
  CASE
    WHEN dialogmote_tidspunkt BETWEEN rapportperiode_start_dato AND rapportperiode_slutt_dato
    THEN 1 -- TODO
    ELSE 0
  END AS DIALOGMOTE_DENNE_PERIODEN_FLAGG,
  enhet_nr,
  periode
  FROM tilfeller_passerer_26u
)

, final AS (
  SELECT tilfeller_med_flagg1.*,
  CASE
    WHEN kandidat_flagg -unntak_flagg -dialogmote_tidligere_periode_flagg  < 1
    THEN 0
    ELSE 1
  END AS Kandidat_uten_untakk_eller_dm_i_tidligere_periode_flagg
  FROM tilfeller_med_flagg1
)

SELECT * FROM final
