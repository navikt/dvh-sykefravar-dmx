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
,dm_2_7 as (
-- copilot er benyttet til å analysere og utvide spørring med flere dialogmøter basert på eksisterende kode
/*
Sjekker for hver dialogmotetidspunkt 1 til 7
1. Hvis dialogmote_tidspunkt = NULL,                             => NULL
2. Hvis dialogmote_tidspunkt > unntaksdato,                      => NULL
3. Hvis (dialogmote_tidspunkt - tilfelle_startdato) > 365 dager, => NULL
4. Hvis unntaksdato = NULL,                                      => dialogmote_tidspunkt
5. Hvis dialogmøtetidspunkt < unntaksdato,                       => dialogmote_tidspunkt
6. Hvis dialogmøtetidspunkt > unntaksdato,                       => NULL eller tidspunkt for forrige dialogmøte
*/
  select fk_person1, tilfelle_startdato,
    CASE
      WHEN (dialogmote_tidspunkt1 is null) or (dialogmote_tidspunkt1 > unntak) or (extract(day from (dialogmote_tidspunkt1 - tilfelle_startdato))) > 365 then null
      WHEN unntak is null then dialogmote_tidspunkt1
      WHEN dialogmote_tidspunkt1 < unntak then dialogmote_tidspunkt1
    END AS dialogmote2_avholdt_dato,
    CASE
      WHEN (dialogmote_tidspunkt2 is null) or extract(day from (dialogmote_tidspunkt2 - tilfelle_startdato)) > 365 then null
      WHEN unntak is null then dialogmote_tidspunkt2
      WHEN dialogmote_tidspunkt1 < unntak then dialogmote_tidspunkt2
      WHEN dialogmote_tidspunkt1 > unntak then dialogmote_tidspunkt1
    END AS dialogmote3_avholdt_dato,
    CASE
      WHEN (dialogmote_tidspunkt3 is null) or extract(day from (dialogmote_tidspunkt3 - tilfelle_startdato)) > 365 then null
      WHEN unntak is null then dialogmote_tidspunkt3
      WHEN dialogmote_tidspunkt2 < unntak then dialogmote_tidspunkt3
      WHEN dialogmote_tidspunkt2 > unntak then dialogmote_tidspunkt2
    END AS dialogmote4_avholdt_dato,
    CASE
      WHEN (dialogmote_tidspunkt4 is null) or extract(day from (dialogmote_tidspunkt4 - tilfelle_startdato)) > 365 then null
      WHEN unntak is null then dialogmote_tidspunkt4
      WHEN dialogmote_tidspunkt3 < unntak then dialogmote_tidspunkt4
      WHEN dialogmote_tidspunkt3 > unntak then dialogmote_tidspunkt3
    END AS dialogmote5_avholdt_dato,
    CASE
      WHEN (dialogmote_tidspunkt5 is null) or extract(day from (dialogmote_tidspunkt5 - tilfelle_startdato)) > 365 then null
      WHEN unntak is null then dialogmote_tidspunkt5
      WHEN dialogmote_tidspunkt4 < unntak then dialogmote_tidspunkt5
      WHEN dialogmote_tidspunkt4 > unntak then dialogmote_tidspunkt4
    END AS dialogmote6_avholdt_dato,
    CASE
      WHEN (dialogmote_tidspunkt6 is null) or extract(day from (dialogmote_tidspunkt6 - tilfelle_startdato)) > 365 then null
      WHEN unntak is null then dialogmote_tidspunkt6
      WHEN dialogmote_tidspunkt5 < unntak then dialogmote_tidspunkt6
      WHEN dialogmote_tidspunkt5 > unntak then dialogmote_tidspunkt5
    END AS dialogmote7_avholdt_dato
  from hendelser
)
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
  FROM hendelser
  LEFT JOIN dim_person1 ON
    hendelser.fk_person1 = dim_person1.fk_person1 AND
    hendelser.tilfelle_startdato BETWEEN dim_person1.gyldig_fra_dato AND dim_person1.gyldig_til_dato
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
)

SELECT * FROM final
