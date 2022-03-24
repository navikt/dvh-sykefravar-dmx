

WITH fak_syfo_dialogmote AS (
    SELECT
        *
    FROM
        {{ ref('fak_syfo_oppfolging_pvt') }}
),
dim_person1 AS (
    SELECT
        *
    FROM
        {{ ref('stg_dmx_data_dim_person1') }}
),
final AS (
    SELECT
          --id,
           fak_syfo_dialogmote.fk_person1,
           fak_syfo_dialogmote.dialogmote_uuid,
           fak_syfo_dialogmote.nyeste_dialogmote,
           fak_syfo_dialogmote.innkalt,
           fak_syfo_dialogmote.innkalt_tidspunkt,
           fak_syfo_dialogmote.nytt_tid_sted,
           fak_syfo_dialogmote.nytt_tid_sted_tidspunkt,
           fak_syfo_dialogmote.ferdigstilt,
           fak_syfo_dialogmote.ferdigstilt_tidspunkt,
           fak_syfo_dialogmote.avlyst,
           fak_syfo_dialogmote.avlyst_tidspunkt,
           fak_syfo_dialogmote.virksomhetsnr,
           fak_syfo_dialogmote.enhet_nr,
           --nav_ident,
           fak_syfo_dialogmote.nyeste_tilfelle_startdato,
           fak_syfo_dialogmote.arbeidstaker_flagg,
           fak_syfo_dialogmote.arbeidsgiver_flagg,
            fak_syfo_dialogmote.sykmelder_flagg,
           fak_syfo_dialogmote.key_dmx,
           --sykmelder_flagg,
           dim_person1.pk_dim_person AS fk_dim_person
    FROM fak_syfo_dialogmote
    LEFT JOIN dim_person1
    ON fak_syfo_dialogmote.fk_person1 = dim_person1.fk_person1
    AND fak_syfo_dialogmote.nyeste_dialogmote BETWEEN dim_person1.gyldig_fra_dato AND dim_person1.gyldig_til_dato
    and fak_syfo_dialogmote.ferdigstilt = 1
)
SELECT
    *
FROM
    final
