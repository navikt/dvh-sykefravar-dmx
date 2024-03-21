with dialogmote as (
    select * from {{ ref('fk_modia__dialogmote_patch') }} -- patch fordi feil i tilfelle_startdato gir feil i join videre
),

virksomhet as (
    select * from {{ ref('felles_dt_p__dim_virksomhet') }}
),

naering as (
    select * from {{ ref('felles_dt_p__dim_naering') }}
),

-- Finner riktig bedrift_naring_primar_kode basert på gyldighetsintervall
-- Kan få ikke-gyldige virksomhetsnr fra Modia. Disse blir null selv om nummer finnes i dim_virksomhet
-- Setter 'Ukjent'-, '00000'- og null-verdier til '-1' for å håndtere join mot dim_naering
-- '00000' ligger i naeringsstandard 'SN2002', og vil filtreres ut av endelig sett dersom ikke satt til '-1'
-- 'Ukjent' har mange ikke-unike rader i dim_naering og vil gi for mange treff dersom ikke satt til '-1'
dialogmoter_join_virksomhet_gyldig_tid as (
    select
        dialogmote.* ,
        case
            when rtrim(bedrift_naring_primar_kode) = 'Ukjent' then '-1'
            when rtrim(bedrift_naring_primar_kode) = '00000' then '-1'
            when rtrim(bedrift_naring_primar_kode) is null then '-1'
            else rtrim(bedrift_naring_primar_kode)
        end as bedrift_naring_primar_kode

    from dialogmote
    left join virksomhet on dialogmote.virksomhetsnr = virksomhet.bedrift_org_nr
        and  dialogmote.tilfelle_startdato BETWEEN virksomhet.gyldig_fra_dato AND virksomhet.gyldig_til_dato --hendelse_tidspunkt ideelt, men feil i fak_dialogmote hvor vi bare ser på tilfelle_startdato?

),

-- Finner riktig naering_kode basert på gyldighetsintervall
-- Bruker nærringsstandard 'SN2007'. Flere treff per bedrift_naring_primar_kode hvis ikke denne settes
-- Har med naering_kode is null i where-clause for å få med rader som ikke får treff i dim_naering/gyldighetsintervallet
dialogmoter_join_naering_gyldig_tid as (
    select
        dialogmoter_join_virksomhet_gyldig_tid.*,
        NVL(pk_dim_naering, -1) as fk_dim_naering
    from dialogmoter_join_virksomhet_gyldig_tid
    left join naering on naering.naering_kode = dialogmoter_join_virksomhet_gyldig_tid.bedrift_naring_primar_kode
        and tilfelle_startdato between naering.gyldig_fra_dato and naering.gyldig_til_dato
    where naeringsstandard='SN2007' or naering_kode is null
),


final as (
    select * from dialogmoter_join_naering_gyldig_tid
)

select * from final
