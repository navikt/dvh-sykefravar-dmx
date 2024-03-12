{{ config(
    materialized='table',
    post_hook= ["grant READ ON dvh_syfo.fak_dialogmote to DVH_SYK_DBT"]
)}}


WITH hendelser AS (
  SELECT * FROM {{ref("mk_dialogmote__pivotert")}}
)

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

,dim_virksomhet as (
    select * from {{ ref('felles_dt_p__dim_virksomhet') }}
)

,dim_naering as (
    select * from {{ ref('felles_dt_p__dim_naering') }}
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
    ,unntak AS unntak_dato
    ,TRUNC(hendelser.tilfelle_startdato + 26*7, 'MM') AS tilfelle_26uker_mnd_startdato
    ,dim_person1.fk_dim_organisasjon
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
      TO_CHAR(unntak, 'YYYYMMDD')
    ), -1) AS fk_dim_tid__unntak_dato
    , NVL(dim_alder.pk_dim_alder, -1) as fk_dim_alder
    , NVL(dim_person1.fk_dim_kjonn, -1) as fk_dim_kjonn
    , rtrim(dim_virksomhet.bedrift_naring_primar_kode) as bedrift_naring_primar_kode
  FROM hendelser
  LEFT JOIN dim_person1 ON
    hendelser.fk_person1 = dim_person1.fk_person1 AND
    hendelser.tilfelle_startdato BETWEEN dim_person1.gyldig_fra_dato AND dim_person1.gyldig_til_dato
    -- Brukers virksomhetsnr, kjønn og organisasjon v/tilfelle_startdato
  LEFT JOIN flagg_innen_26Uker ON
    hendelser.fk_person1 = flagg_innen_26Uker.fk_person1 AND
    hendelser.tilfelle_startdato = flagg_innen_26Uker.tilfelle_startdato
  LEFT JOIN dim_organisasjon ON
    dim_person1.fk_dim_organisasjon = dim_organisasjon.pk_dim_organisasjon
  LEFT JOIN dim_org ON
    dim_organisasjon.mapping_node_kode = dim_org.mapping_node_kode AND
    dim_org.funk_gyldig_til_dato = TO_DATE('9999-12-31', 'YYYY-MM-DD') AND -- TODO: Bør settes på en annen måte
    dim_org.mapping_node_type = 'NORGENHET'
  LEFT JOIN motebehov ON
    hendelser.fk_person1 = motebehov.fk_person1 AND
    hendelser.tilfelle_startdato = motebehov.tilfelle_startdato
  LEFT JOIN dim_alder ON
   -- dim_alder.alder = TRUNC(MONTHS_BETWEEN(hendelser.tilfelle_startdato, dim_person1.fodt_dato)/12) -- Brukt til månedlig rapportering - feil?
    dim_alder.alder = floor((hendelser.tilfelle_startdato-dim_person1.fodt_dato)/365.25)
    and hendelser.tilfelle_startdato between dim_person1.gyldig_fra_dato AND dim_person1.gyldig_til_dato
  LEFT JOIN dim_virksomhet ON
    hendelser.virksomhetsnr = dim_virksomhet.bedrift_org_nr
        and hendelser.tilfelle_startdato BETWEEN dim_virksomhet.gyldig_fra_dato AND dim_virksomhet.gyldig_til_dato

  )

,final as (
  select
    fk_person1,
    tilfelle_startdato,
    virksomhetsnr,
    dm2_innen_26_uker_flagg,
    behov_meldt_dato,
    behov_sykmeldt,
    behov_arbeidsgiver,
    dialogmote2_avholdt_dato,
    dialogmote3_avholdt_dato,
    dialogmote4_avholdt_dato,
    dialogmote5_avholdt_dato,
    dialogmote6_avholdt_dato,
    dialogmote7_avholdt_dato,
    unntak_dato,
    tilfelle_26uker_mnd_startdato,
    fk_dim_organisasjon,
    fk_dim_tid__behov_meldt,
    fk_dim_tid__tilfelle_startdato,
    fk_dim_tid__dm2_avholdt_dato,
    fk_dim_tid__dm3_avholdt_dato,
    fk_dim_tid__dm4_avholdt_dato,
    fk_dim_tid__dm5_avholdt_dato,
    fk_dim_tid__dm6_avholdt_dato,
    fk_dim_tid__dm7_avholdt_dato,
    fk_dim_tid__unntak_dato,
    fk_dim_alder,
    fk_dim_kjonn,
    dim_naering.naering_kode as naering_kode
  from joined
  left join dim_naering on dim_naering.naering_kode = joined.bedrift_naring_primar_kode
    and joined.tilfelle_startdato between dim_naering.gyldig_fra_dato AND dim_naering.gyldig_til_dato

)


SELECT * FROM final


