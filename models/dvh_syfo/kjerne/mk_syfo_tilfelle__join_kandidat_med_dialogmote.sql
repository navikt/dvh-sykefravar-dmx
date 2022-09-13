WITH kandidat AS (
  SELECT * FROM {{ ref('fk_modia__kandidat') }}
)

, dialogmote AS (
  SELECT * FROM {{ ref('fk_modia__dialogmote__dummy__fix202210') }}
)

, kandidat_filtrert AS (
  SELECT
    *
  FROM kandidat
  WHERE personidentnumber NOT IN (
    SELECT personidentnumber FROM kandidat WHERE kandidat = 0
  )
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
      kandidat_filtrert.personidentnumber, NULL, dialogmote_gruppert.person_ident_number,
      kandidat_filtrert.personidentnumber
    ) AS person_ident_number
    ,DECODE(
      kandidat_filtrert.tilfellestartdato, NULL, dialogmote_gruppert.tilfelle_startdato,
      kandidat_filtrert.tilfellestartdato
    ) AS tilfelle_startdato
    ,kandidat_filtrert.uuid AS kandidat_uuid
    ,kandidat_filtrert.createdAt AS kandidat_created_at
    ,kandidat_filtrert.kandidat
    ,kandidat_filtrert.arsak AS kandidat_arsak
    ,dialogmote_gruppert.dialogmote_uuid
    ,dialogmote_gruppert.dialogmote_tidspunkt
    ,dialogmote_gruppert.enhet_nr
    ,dialogmote_gruppert.arbeidstaker
    ,dialogmote_gruppert.arbeidsgiver
    ,dialogmote_gruppert.sykmelder
  FROM kandidat_filtrert
  FULL OUTER JOIN dialogmote_gruppert ON
    kandidat_filtrert.personidentnumber = dialogmote_gruppert.person_ident_number AND
    kandidat_filtrert.tilfellestartdato = dialogmote_gruppert.tilfelle_startdato
)

, final AS (
  SELECT * FROM tilfeller
)

SELECT * FROM final
