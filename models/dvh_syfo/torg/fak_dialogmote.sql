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
  SELECT * FROM {{ ref('felles_dt_p__dim_organisasjon') }}
)

,dim_org AS (
  SELECT * FROM {{ ref('felles_dt_p__dim_org') }}
)

,motebehov AS (
  SELECT * FROM {{ ref('mk_motebehov__prepp') }}
)

,dim_alder as (
  select * from {{ ref('felles_dt_p__dim_alder') }}
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
Setter dialogmote3_avholdt_dato basert på reglene:
1. Hvis dialogmote_tidspunkt2 = NULL,                             => NULL
2. Hvis (dialogmote_tidspunkt2 - tilfelle_startdato) > 365 dager, => NULL
3. Hvis dialogmote2_avholdt_dato = NULL,                          => dialogmote_tidspunkt1
4. Ellers,                                                        => dialogmote_tidspunkt2
*/
  select dm_2.*,
    CASE
      WHEN ((dm_2.dialogmote_tidspunkt2 is null) or extract(day from (dm_2.dialogmote_tidspunkt2 - dm_2.tilfelle_startdato)) > 365 )
        and ((dm_2.dialogmote_tidspunkt1 is null) or extract(day from (dm_2.dialogmote_tidspunkt1 - dm_2.tilfelle_startdato)) > 365 ) then null
      WHEN dm_2.dialogmote2_avholdt_dato is null then dm_2.dialogmote_tidspunkt1
      ELSE dialogmote_tidspunkt2
    END AS dialogmote3_avholdt_dato
  from dm_2
  ),

dm_4 as (
/*
Setter dialogmote4_avholdt_dato basert på reglene:
1. Hvis dialogmote_tidspunkt3 = NULL,                             => NULL
2. Hvis (dialogmote_tidspunkt3 - tilfelle_startdato) > 365 dager, => NULL
3. Hvis dialogmote2_avholdt_dato = NULL,                          => dialogmote_tidspunkt2
4. Ellers,                                                        => dialogmote_tidspunkt3
*/
    select dm_3.*,
    CASE
      WHEN ((dm_3.dialogmote_tidspunkt3 is null) or extract(day from (dm_3.dialogmote_tidspunkt3 - dm_3.tilfelle_startdato)) > 365 )
        and ((dm_3.dialogmote_tidspunkt2 is null) or extract(day from (dm_3.dialogmote_tidspunkt2 - dm_3.tilfelle_startdato)) > 365 ) then null
      WHEN dm_3.dialogmote2_avholdt_dato is null then dm_3.dialogmote_tidspunkt2
      ELSE dialogmote_tidspunkt3
    END AS dialogmote4_avholdt_dato
    from dm_3
    ),

dm_5 as (
/*
Setter dialogmote5_avholdt_dato basert på reglene:
1. Hvis dialogmote_tidspunkt4 = NULL,                             => NULL
2. Hvis (dialogmote_tidspunkt4 - tilfelle_startdato) > 365 dager, => NULL
3. Hvis dialogmote2_avholdt_dato = NULL,                          => dialogmote_tidspunkt3
4. Ellers,                                                        => dialogmote_tidspunkt4
*/
  select dm_4.*,
    CASE
      WHEN ((dm_4.dialogmote_tidspunkt4 is null) or extract(day from (dm_4.dialogmote_tidspunkt4 - dm_4.tilfelle_startdato)) > 365 )
        and ((dm_4.dialogmote_tidspunkt3 is null) or extract(day from (dm_4.dialogmote_tidspunkt3 - dm_4.tilfelle_startdato)) > 365 ) then null
      WHEN dm_4.dialogmote2_avholdt_dato is null then dm_4.dialogmote_tidspunkt3
      else dialogmote_tidspunkt4
      END AS dialogmote5_avholdt_dato
      from dm_4
      ),

dm_6 as (
/*
Setter dialogmote6_avholdt_dato basert på reglene:
1. Hvis dialogmote_tidspunkt5 = NULL,                             => NULL
2. Hvis (dialogmote_tidspunkt5 - tilfelle_startdato) > 365 dager, => NULL
3. Hvis dialogmote2_avholdt_dato = NULL,                          => dialogmote_tidspunkt4
4. Ellers,                                                        => dialogmote_tidspunkt5
*/
  select dm_5.*,
    CASE
      WHEN ((dm_5.dialogmote_tidspunkt5 is null) or extract(day from (dm_5.dialogmote_tidspunkt5 - dm_5.tilfelle_startdato)) > 365 )
        and ((dm_5.dialogmote_tidspunkt4 is null) or extract(day from (dm_5.dialogmote_tidspunkt4 - dm_5.tilfelle_startdato)) > 365) then null
      WHEN dm_5.dialogmote2_avholdt_dato is null then dm_5.dialogmote_tidspunkt4
      else dialogmote_tidspunkt5
    END AS dialogmote6_avholdt_dato
  from dm_5
  ),

dm_7 as (
/*
Setter dialogmote7_avholdt_dato basert på reglene:
1. Hvis dialogmote_tidspunkt6 = NULL,                             => NULL
2. Hvis (dialogmote_tidspunkt6 - tilfelle_startdato) > 365 dager, => NULL
3. Hvis dialogmote2_avholdt_dato = NULL,                          => dialogmote_tidspunkt5
4. Ellers,                                                        => dialogmote_tidspunkt6
*/
  select dm_6.*,
    CASE
      WHEN ((dm_6.dialogmote_tidspunkt6 is null) or extract(day from (dm_6.dialogmote_tidspunkt6 - dm_6.tilfelle_startdato)) > 365 )
        and ((dm_6.dialogmote_tidspunkt5 is null) or extract(day from (dm_6.dialogmote_tidspunkt5 - dm_6.tilfelle_startdato)) > 365 ) then null
      WHEN dm_6.dialogmote2_avholdt_dato is null then dm_6.dialogmote_tidspunkt5
      else dialogmote_tidspunkt6
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
,flag_innen_26Uker AS (
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
,final AS (
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
    ,dim_org.ek_dim_org
    ,dim_person1.fk_dim_organisasjon
    ,TO_NUMBER(
      TO_CHAR(motebehov.behov_meldt_dato, 'YYYYMMDD')
    ) AS fk_dim_tid__behov_meldt
    ,TO_NUMBER(
      TO_CHAR(hendelser.tilfelle_startdato, 'YYYYMMDD')
    ) AS fk_dim_tid__tilfelle_startdato
    ,TO_NUMBER(
      TO_CHAR(dialogmote2_avholdt_dato, 'YYYYMMDD')
    ) AS fk_dim_tid__dm2_avholdt_dato
    ,TO_NUMBER(
      TO_CHAR(dialogmote3_avholdt_dato, 'YYYYMMDD')
    ) AS fk_dim_tid__dm3_avholdt_dato
    ,TO_NUMBER(
      TO_CHAR(dialogmote4_avholdt_dato, 'YYYYMMDD')
    ) AS fk_dim_tid__dm4_avholdt_dato
    ,TO_NUMBER(
      TO_CHAR(dialogmote5_avholdt_dato, 'YYYYMMDD')
    ) AS fk_dim_tid__dm5_avholdt_dato
    ,TO_NUMBER(
      TO_CHAR(dialogmote6_avholdt_dato, 'YYYYMMDD')
    ) AS fk_dim_tid__dm6_avholdt_dato
    ,TO_NUMBER(
      TO_CHAR(dialogmote7_avholdt_dato, 'YYYYMMDD')
    ) AS fk_dim_tid__dm7_avholdt_dato
    ,TO_NUMBER(
      TO_CHAR(unntak, 'YYYYMMDD')
    ) AS fk_dim_tid__unntak_dato
    , NVL(dim_alder.pk_dim_alder, -1) as fk_dim_alder
    , NVL(dim_person1.fk_dim_kjonn, -1) as fk_dim_kjonn
  FROM hendelser
  LEFT JOIN dim_person1 ON
    hendelser.fk_person1 = dim_person1.fk_person1 AND
    hendelser.tilfelle_startdato BETWEEN dim_person1.gyldig_fra_dato AND dim_person1.gyldig_til_dato
    -- Brukers virksomhetsnr, kjønn og organisasjon v/tilfelle_startdato
  LEFT JOIN flag_innen_26Uker ON
    hendelser.fk_person1 = flag_innen_26Uker.fk_person1 AND
    hendelser.tilfelle_startdato = flag_innen_26Uker.tilfelle_startdato
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

  )

SELECT * FROM final


