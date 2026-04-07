{{ config(
    materialized='incremental',
    unique_key=['fk_person1', 'tilfelle_startdato'],
    incremental_strategy='delete+insert',
    post_hook= ["grant READ ON dvh_syfo.fak_dialogmote to DVH_SYK_DBT"]
)}}


WITH hendelser AS (
  SELECT * FROM {{ ref('mk_dialogmote__pivotert')}}
  {% if is_incremental() %}
  -- Filtrerer på tilfeller hvor:
  -- 1. Nye tilfeller siste 180 dager, ELLER
  -- 2. Tilfeller med aktivitet (dialogmøter/unntak) siste 7 dager
  WHERE (
    tilfelle_startdato >= TRUNC(SYSDATE) - 180
    OR
    GREATEST(
      NVL(dialogmote_tidspunkt1, TO_DATE('1900-01-01', 'YYYY-MM-DD')),
      NVL(dialogmote_tidspunkt2, TO_DATE('1900-01-01', 'YYYY-MM-DD')),
      NVL(dialogmote_tidspunkt3, TO_DATE('1900-01-01', 'YYYY-MM-DD')),
      NVL(dialogmote_tidspunkt4, TO_DATE('1900-01-01', 'YYYY-MM-DD')),
      NVL(dialogmote_tidspunkt5, TO_DATE('1900-01-01', 'YYYY-MM-DD')),
      NVL(dialogmote_tidspunkt6, TO_DATE('1900-01-01', 'YYYY-MM-DD')),
      NVL(unntak, TO_DATE('1900-01-01', 'YYYY-MM-DD')),
      NVL(stoppunkt, TO_DATE('1900-01-01', 'YYYY-MM-DD'))
    ) >= CURRENT_DATE - 7
  )
  {% endif %}
)

/*
-- Henter Nav enhet og hekter på ek_org_node
,hente_tildelt_enhet as (
  select enhet.fk_person1,
         enhet.tildelt_enhet,
         enhet.gyldig_fra_dato,
         enhet.gyldig_til_dato,
         org.ek_org_node
  from {{ source('modia', 'stg_modia__person_tildelt_enhet') }} enhet
  inner join {{ source('dt_kodeverk', 'org_enhet_til_node') }} org
          on org.enhet_kode = enhet.tildelt_enhet
          and org.enhet_type = 'NORGENHET'
) */

-- Henter fk_dim_organisasjon fra dim_person1
-- Brukes når vi ikke har tildelt_enhet i
-- stg_modia__person_tildelt_enhet-tabellen
,dim_person1 AS (
  SELECT * FROM {{ ref('felles_dt_person__dim_person1') }}
)

,dim_organisasjon AS (
  SELECT * FROM {{ ref('felles_dt_kodeverk__dim_organisasjon') }}
)

,dim_org AS (
  SELECT * FROM {{ ref('felles_dt_kodeverk__dim_org') }}
)

,motebehov AS (
  SELECT * FROM {{ ref('mk_motebehov__prepp') }}
)

,dim_alder as (
  select * from {{ ref('felles_dt_kodeverk__dim_alder') }}
)

,hendelser_med_naering as (
  select * from {{ ref('mk_dialogmote__naering_ved_tilfelle_startdato') }}
)

,veileder as (
  select * from {{ ref('felles_dt_hr__hr_navkontor_ansatt') }}
)

,org as (
  select * from {{ source('dt_kodeverk', 'org_enhet_til_node') }}
)

,dm_2 as (
/*
Setter dialogmote2_avholdt_dato basert på reglene:
1. Hvis dialogmote_tidspunkt1 = NULL,                             => NULL
2. Hvis dialogmote_tidspunkt1 > unntak,                           => NULL (det er bare å få unntak kun for dialogmøte 2)
3. Hvis (dialogmote_tidspunkt1 - tilfelle_startdato) > 365 dager, => NULL
4. Hvis unntak = NULL,                                            => dialogmote_tidspunkt1
5. Hvis dialogmote_tidspunkt1 < unntak,                           => dialogmote_tidspunkt1
*/
  select fk_person1,
         tilfelle_startdato,
         dialogmote_tidspunkt1,
         dialogmote_tidspunkt2,
         dialogmote_tidspunkt3,
         dialogmote_tidspunkt4,
         dialogmote_tidspunkt5,
         dialogmote_tidspunkt6,
    CASE
      WHEN (dialogmote_tidspunkt1 is null) or (dialogmote_tidspunkt1 > unntak)
                                           or (extract(day from (dialogmote_tidspunkt1 - tilfelle_startdato))) > 365 then null
      WHEN unntak is null then dialogmote_tidspunkt1
      WHEN dialogmote_tidspunkt1 < unntak then dialogmote_tidspunkt1
    END AS dialogmote2_avholdt_dato
  from dvh_syfo.mk_dialogmote__pivotert
  ),

dm_3 as (
/*
Setter dialogmoteX_avholdt_dato basert på reglene:
1. Hvis dialogmote2_avholdt_dato = NULL (unntak) og tidspunkt for dialogmøtehendelse er under 365 dager siden sykefraværtilfelle => dialogmote_tidspunkt_(X-1)
2. Hvis dialogmote2_avholdt_dato != NULL (ikke unntak) og tidspunkt for dialogmøtehendelse er under 365 dager siden sykefraværtilfelle => dialogmote_tidspunkt_X
2. Ellers NULL
*/
  select dm_2.*,
    CASE
      WHEN dialogmote2_avholdt_dato is null and extract(day from (dialogmote_tidspunkt1 - tilfelle_startdato)) <= 365 then dialogmote_tidspunkt1
      WHEN dialogmote2_avholdt_dato is not null and extract(day from (dialogmote_tidspunkt2 - tilfelle_startdato)) <= 365 then dialogmote_tidspunkt2
      else null
    END AS dialogmote3_avholdt_dato
  from dm_2
  ),

dm_4 as (

  select dm_3.*,
    CASE
      WHEN dialogmote2_avholdt_dato is null and extract(day from (dialogmote_tidspunkt2 - tilfelle_startdato)) <= 365 then dialogmote_tidspunkt2
      WHEN dialogmote2_avholdt_dato is not null and extract(day from (dialogmote_tidspunkt3 - tilfelle_startdato)) <= 365 then dialogmote_tidspunkt3
      else null
    END AS dialogmote4_avholdt_dato
  from dm_3
  ),

dm_5 as (

  select dm_4.*,
    CASE
      WHEN dialogmote2_avholdt_dato is null and extract(day from (dialogmote_tidspunkt3 - tilfelle_startdato)) <= 365 then dialogmote_tidspunkt3
      WHEN dialogmote2_avholdt_dato is not null and extract(day from (dialogmote_tidspunkt4 - tilfelle_startdato)) <= 365 then dialogmote_tidspunkt4
      else null
    END AS dialogmote5_avholdt_dato
  from dm_4
  ),

dm_6 as (

  select dm_5.*,
    CASE
      WHEN dialogmote2_avholdt_dato is null and extract(day from (dialogmote_tidspunkt4 - tilfelle_startdato)) <= 365 then dialogmote_tidspunkt4
      WHEN dialogmote2_avholdt_dato is not null and extract(day from (dialogmote_tidspunkt5 - tilfelle_startdato)) <= 365 then dialogmote_tidspunkt5
      else null
    END AS dialogmote6_avholdt_dato
  from dm_5
  ),

dm_7 as (

  select dm_6.*,
    CASE
      WHEN dialogmote2_avholdt_dato is null and extract(day from (dialogmote_tidspunkt5 - tilfelle_startdato)) <= 365 then dialogmote_tidspunkt5
      WHEN dialogmote2_avholdt_dato is not null and extract(day from (dialogmote_tidspunkt6 - tilfelle_startdato)) <= 365 then dialogmote_tidspunkt6
      else null
    END AS dialogmote7_avholdt_dato
  from dm_6
  ),

dm_2_7 as (
/*
Samler alle dialogmote_avholdt_dato fra dm_2 til dm_7
*/
  select fk_person1,
         tilfelle_startdato,
         dialogmote2_avholdt_dato,
         dialogmote3_avholdt_dato,
         dialogmote4_avholdt_dato,
         dialogmote5_avholdt_dato,
         dialogmote6_avholdt_dato,
         dialogmote7_avholdt_dato
  from dm_7
)
-- Trengs dm3-7 i SELECT her?
,flagg_innen_26Uker AS (
  SELECT fk_person1,
         tilfelle_startdato,
         dialogmote2_avholdt_dato,
         dialogmote3_avholdt_dato,
         dialogmote4_avholdt_dato,
         dialogmote5_avholdt_dato,
         dialogmote6_avholdt_dato,
         dialogmote7_avholdt_dato,
    CASE
      WHEN dialogmote2_avholdt_dato IS NULL THEN NULL
      WHEN dialogmote2_avholdt_dato < (tilfelle_startdato + 26*7) THEN 1
      ELSE 0
    END AS dialogmote2_innen_26_uker_flagg
  FROM dm_2_7
)



,joined AS (
  SELECT
    hendelser.fk_person1
    ,hendelser.tilfelle_startdato
    ,hendelser.virksomhetsnr
    ,dialogmote2_innen_26_uker_flagg AS dm2_innen_26_uker_flagg
    ,behov_meldt_dato
    ,behov_sykmeldt
    ,behov_arbeidsgiver
    ,dialogmote2_avholdt_dato
    ,dialogmote3_avholdt_dato
    ,dialogmote4_avholdt_dato
    ,dialogmote5_avholdt_dato
    ,dialogmote6_avholdt_dato
    ,dialogmote7_avholdt_dato
    ,hendelser.unntak AS unntak_dato
    ,hendelser.unntakarsak_modia
    ,TRUNC(hendelser.tilfelle_startdato + 26*7, 'DD') AS tilfelle_26uker_mnd_startdato
    ,NVL(dim_person1.fk_dim_organisasjon, -1) as fk_dim_organisasjon
    --,coalesce(d.ek_org_node, dim_person1.fk_dim_organisasjon) as fk_dim_org -------NY VARIANT, MÅ TESTES BEDRE DA DET FØRER MED STORE ENDRINGER (hver 10. får ny nøkkel)
    ,NVL(TO_NUMBER(
      TO_CHAR(motebehov.behov_meldt_dato, 'YYYYMMDD')
    ), -1) AS fk_dim_tid__behov_meldt
    ,NVL(TO_NUMBER(
      TO_CHAR(hendelser.tilfelle_startdato, 'YYYYMMDD')
    ), -1) AS fk_dim_tid__tilfelle_startdato
    ,NVL(TO_NUMBER(
      TO_CHAR(dialogmote2_avholdt_dato, 'YYYYMMDD')
    ), -1) AS fk_dim_tid__dm2_avholdt_dato
    ,NVL(TO_NUMBER(
      TO_CHAR(dialogmote3_avholdt_dato, 'YYYYMMDD')
    ), -1) AS fk_dim_tid__dm3_avholdt_dato
    ,NVL(TO_NUMBER(
      TO_CHAR(dialogmote4_avholdt_dato, 'YYYYMMDD')
    ), -1) AS fk_dim_tid__dm4_avholdt_dato
    ,NVL(TO_NUMBER(
      TO_CHAR(dialogmote5_avholdt_dato, 'YYYYMMDD')
    ), -1) AS fk_dim_tid__dm5_avholdt_dato
    ,NVL(TO_NUMBER(
      TO_CHAR(dialogmote6_avholdt_dato, 'YYYYMMDD')
    ), -1) AS fk_dim_tid__dm6_avholdt_dato
    ,NVL(TO_NUMBER(
      TO_CHAR(dialogmote7_avholdt_dato, 'YYYYMMDD')
    ), -1) AS fk_dim_tid__dm7_avholdt_dato
    ,NVL(TO_NUMBER(
      TO_CHAR(hendelser.unntak, 'YYYYMMDD')
    ), -1) AS fk_dim_tid__unntak_dato
    , NVL(dim_alder.pk_dim_alder, -1) as fk_dim_alder
    , NVL(dim_person1.fk_dim_kjonn, -1) as fk_dim_kjonn
    , NVL(fk_dim_naering, -1) as fk_dim_naering
    , NVL(org.ek_org_node, -1) as fk_dim_org_veileder
    , hendelser.region_oppf_enhet_vviken_flagg as region_oppf_enhet_vviken_flagg
    , hendelser.kildesystem
  FROM hendelser
  LEFT JOIN dim_person1 ON
    hendelser.fk_person1 = dim_person1.fk_person1 AND
    hendelser.tilfelle_startdato BETWEEN dim_person1.gyldig_fra_dato AND dim_person1.gyldig_til_dato
    -- Brukers virksomhetsnr, kjønn og organisasjon v/tilfelle_startdato
  LEFT JOIN flagg_innen_26Uker ON
    hendelser.fk_person1 = flagg_innen_26Uker.fk_person1 AND
    hendelser.tilfelle_startdato = flagg_innen_26Uker.tilfelle_startdato
/*  LEFT JOIN hente_tildelt_enhet d
        ON d.fk_person1 = hendelser.fk_person1
        AND trunc(hendelser.tilfelle_startdato) BETWEEN trunc(d.gyldig_fra_dato) AND trunc(d.gyldig_til_dato) */
  LEFT JOIN dim_organisasjon ON
    dim_person1.fk_dim_organisasjon = dim_organisasjon.pk_dim_organisasjon
 LEFT JOIN dim_org ON
    dim_organisasjon.mapping_node_kode = dim_org.mapping_node_kode
    AND trunc(hendelser.tilfelle_startdato) BETWEEN trunc(dim_org.funk_gyldig_fra_dato) AND trunc(dim_org.funk_gyldig_til_dato)  --RIKTIG MÅTE
    --dim_org.funk_gyldig_til_dato = TO_DATE('9999-12-31', 'YYYY-MM-DD') AND -- TODO: Bør settes på en annen måte
    AND dim_org.mapping_node_type = 'NORGENHET'
  LEFT JOIN motebehov ON
    hendelser.fk_person1 = motebehov.fk_person1 AND
    hendelser.tilfelle_startdato = motebehov.tilfelle_startdato
  LEFT JOIN dim_alder ON
   -- dim_alder.alder = TRUNC(MONTHS_BETWEEN(hendelser.tilfelle_startdato, dim_person1.fodt_dato)/12) -- Brukt til månedlig rapportering - feil?
    dim_alder.alder = floor((hendelser.tilfelle_startdato-dim_person1.fodt_dato)/365.25)
    and hendelser.tilfelle_startdato between dim_person1.gyldig_fra_dato AND dim_person1.gyldig_til_dato
  LEFT JOIN hendelser_med_naering ON
    hendelser.fk_person1 = hendelser_med_naering.fk_person1
    and hendelser.tilfelle_startdato = hendelser_med_naering.tilfelle_startdato
  LEFT JOIN veileder ON
    hendelser.nav_ident = veileder.nav_id
    and hendelser.tilfelle_startdato between veileder.funksjonell_fra_dato and veileder.funksjonell_til_dato
  LEFT JOIN org ON
    veileder.nav_kontor_stilling = org.enhet_kode
    and org.enhet_type = 'NORGENHET'
  )

,final as (
  select
    CAST(fk_person1                         AS NUMBER(38,0))       AS fk_person1,
    CAST(tilfelle_startdato                 AS DATE)               AS tilfelle_startdato,
    CAST(virksomhetsnr                      AS VARCHAR2(100))      AS virksomhetsnr,
    CAST(dm2_innen_26_uker_flagg            AS NUMBER(1,0))        AS dm2_innen_26_uker_flagg,
    CAST(behov_meldt_dato                   AS DATE)               AS behov_meldt_dato,
    CAST(behov_sykmeldt                     AS NUMBER(1,0))        AS behov_sykmeldt,
    CAST(behov_arbeidsgiver                 AS NUMBER(1,0))        AS behov_arbeidsgiver,
    CAST(dialogmote2_avholdt_dato           AS DATE)               AS dialogmote2_avholdt_dato,
    CAST(dialogmote3_avholdt_dato           AS DATE)               AS dialogmote3_avholdt_dato,
    CAST(dialogmote4_avholdt_dato           AS DATE)               AS dialogmote4_avholdt_dato,
    CAST(dialogmote5_avholdt_dato           AS DATE)               AS dialogmote5_avholdt_dato,
    CAST(dialogmote6_avholdt_dato           AS DATE)               AS dialogmote6_avholdt_dato,
    CAST(dialogmote7_avholdt_dato           AS DATE)               AS dialogmote7_avholdt_dato,
    CAST(unntak_dato                        AS DATE)               AS unntak_dato,
    CAST(unntakarsak_modia                  AS VARCHAR2(100))      AS unntakarsak_modia,
    CAST(tilfelle_26uker_mnd_startdato      AS DATE)             AS tilfelle_26uker_mnd_startdato,
    --CAST(fk_dim_org                         AS NUMBER(38,0))       AS fk_dim_org,
    CAST(fk_dim_organisasjon                AS NUMBER(38,0))       AS fk_dim_organisasjon,
    CAST(fk_dim_org_veileder                AS NUMBER(38,0))       AS fk_dim_org_veileder,
    CAST(fk_dim_tid__behov_meldt            AS NUMBER(38,0))       AS fk_dim_tid__behov_meldt,
    CAST(fk_dim_tid__tilfelle_startdato     AS NUMBER(38,0))       AS fk_dim_tid__tilfelle_startdato,
    CAST(fk_dim_tid__dm2_avholdt_dato       AS NUMBER(38,0))       AS fk_dim_tid__dm2_avholdt_dato,
    CAST(fk_dim_tid__dm3_avholdt_dato       AS NUMBER(38,0))       AS fk_dim_tid__dm3_avholdt_dato,
    CAST(fk_dim_tid__dm4_avholdt_dato       AS NUMBER(38,0))       AS fk_dim_tid__dm4_avholdt_dato,
    CAST(fk_dim_tid__dm5_avholdt_dato       AS NUMBER(38,0))       AS fk_dim_tid__dm5_avholdt_dato,
    CAST(fk_dim_tid__dm6_avholdt_dato       AS NUMBER(38,0))       AS fk_dim_tid__dm6_avholdt_dato,
    CAST(fk_dim_tid__dm7_avholdt_dato       AS NUMBER(38,0))       AS fk_dim_tid__dm7_avholdt_dato,
    CAST(fk_dim_tid__unntak_dato            AS NUMBER(38,0))       AS fk_dim_tid__unntak_dato,
    CAST(fk_dim_alder                       AS NUMBER(38,0))       AS fk_dim_alder,
    CAST(fk_dim_kjonn                       AS NUMBER(38,0))       AS fk_dim_kjonn,
    CAST(fk_dim_naering                     AS NUMBER(38,0))       AS fk_dim_naering,
    CAST(region_oppf_enhet_vviken_flagg     AS NUMBER(1,0))        AS region_oppf_enhet_vviken_flagg,
    CAST(kildesystem                        AS VARCHAR2(100))      AS kildesystem,
    sysdate AS oppdatert_dato,
    sysdate AS lastet_dato

  from joined
)

SELECT * FROM final