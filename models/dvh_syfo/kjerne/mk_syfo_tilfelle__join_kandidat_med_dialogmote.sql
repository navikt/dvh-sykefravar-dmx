WITH kandidat AS (
  SELECT * FROM {{ ref('fk_modia__kandidat') }}
)

, dialogmote AS (
  SELECT * FROM {{ ref('fk_modia__dialogmote__dummy__fix202210') }}
)

, kandidat_gruppert AS (
  SELECT
    personidentnumber
    ,tilfellestartdato
  FROM kandidat
  WHERE
    kandidat = 1 -- TODO: Test om det her kan få konsekvenser
  GROUP BY
    personidentnumber
    ,tilfellestartdato
)

, dialogmote_gruppert AS (
  SELECT
    dialogmote_uuid
    ,dialogmote_tidspunkt
    ,person_ident_number
    ,enhet_nr
    ,tilfelle_startdato
    ,arbeidstaker
    ,arbeidsgiver
    ,sykmelder
  FROM
    dialogmote
  GROUP BY
    dialogmote_uuid
    ,dialogmote_tidspunkt
    ,person_ident_number
    ,enhet_nr
    ,tilfelle_startdato
    ,arbeidstaker
    ,arbeidsgiver
    ,sykmelder
)

, tilfeller AS (
  SELECT
    DECODE(
      kandidat_gruppert.personidentnumber, NULL, dialogmote_gruppert.person_ident_number,
      kandidat_gruppert.personidentnumber
    ) AS person_ident_number
    ,DECODE(
      kandidat_gruppert.tilfellestartdato, NULL, dialogmote_gruppert.tilfelle_startdato,
      kandidat_gruppert.tilfellestartdato
    ) AS tilfelle_startdato
    ,dialogmote_gruppert.dialogmote_uuid
    ,dialogmote_gruppert.dialogmote_tidspunkt
    ,dialogmote_gruppert.enhet_nr
    ,dialogmote_gruppert.arbeidstaker
    ,dialogmote_gruppert.arbeidsgiver
    ,dialogmote_gruppert.sykmelder
  FROM kandidat_gruppert
  FULL OUTER JOIN dialogmote_gruppert ON
    kandidat_gruppert.personidentnumber = dialogmote_gruppert.person_ident_number AND
    kandidat_gruppert.tilfellestartdato = dialogmote_gruppert.tilfelle_startdato
)

, final AS (
  SELECT * FROM tilfeller
)

SELECT * FROM final
