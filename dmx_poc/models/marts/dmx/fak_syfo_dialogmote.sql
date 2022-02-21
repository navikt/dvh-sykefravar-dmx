WITH isdialogmote AS (
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
resultat AS (
    SELECT
        id,
        isdialogmote.fk_person1,
        dialogmote_uuid,
        dialogmote_tidspunkt,
        status_endring_type,
        status_endring_tidspunkt,
        virksomhetsnr,
        enhet_nr,
        nav_ident,
        tilfelle_startdato,
        arbeidstaker_flagg,
        arbeidsgiver_flagg,
        sykmelder_flagg,
        dim_person1.pk_dim_person AS fk_dim_person
    FROM
        isdialogmote
        JOIN dim_person1
        ON isdialogmote.fk_person1 = dim_person1.fk_person1
        AND isdialogmote.dialogmote_tidspunkt BETWEEN dim_person1.gyldig_fra_dato
        AND dim_person1.gyldig_til_dato
)
SELECT
    *
FROM
    resultat
