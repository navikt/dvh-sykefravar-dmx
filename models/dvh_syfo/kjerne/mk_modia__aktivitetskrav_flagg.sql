WITH aktivitetskrav_mk as (
  SELECT a.*, last_day(LASTET_DATO) as siste_dag_i_mnd
  FROM {{ ref("mk_modia__aktivitetskrav") }} a
),

sykefravar_med_flagg as (
  select aktivitetskrav_mk.*,
   CASE
    WHEN ARSAKER = 'FRISKMELDT' OR ARSAKER1= 'FRISKMELDT' OR ARSAKER2 = 'FRISKMELDT'  THEN 1
    ELSE 0
  END AS friskmeldt_flagg,
  CASE
    WHEN ARSAKER = 'GRADERT' OR ARSAKER1= 'GRADERT' OR ARSAKER2 = 'GRADERT' THEN 1
    ELSE 0
  END AS gradert_flagg,
  CASE
    WHEN ARSAKER = 'TILRETTELEGGING_IKKE_MULIG' OR ARSAKER1= 'TILRETTELEGGING_IKKE_MULIG' OR ARSAKER2 = 'TILRETTELEGGING_IKKE_MULIG' THEN 1
    ELSE 0
  END AS tilrettelegging_ikke_mulig_flagg,
  CASE
    WHEN ARSAKER = 'SJOMENN_UTENRIKS' OR ARSAKER1= 'SJOMENN_UTENRIKS' OR ARSAKER2 = 'SJOMENN_UTENRIKS' THEN 1
    ELSE 0
  END AS sjomenn_utenriks_flagg,
  CASE
    WHEN ARSAKER = 'MEDISINSKE_GRUNNER' OR ARSAKER1= 'MEDISINSKE_GRUNNER' OR ARSAKER2 = 'MEDISINSKE_GRUNNER' THEN 1
    ELSE 0
  END AS medisinske_grunner_flagg,
  CASE
    WHEN ARSAKER NOT IN
    ('FRISKMELDT', 'GRADERT', 'MEDISINSKE_GRUNNER','SJOMENN_UTENRIKS','TILRETTELEGGING_IKKE_MULIG') THEN 1
    ELSE 0
  END AS ukjent_blank_flagg
FROM aktivitetskrav_mk

),

oversikt_status_scd as (
  select FK_PERSON1, TILDELT_ENHET, DBT_VALID_TO, DBT_VALID_FROM,
                ROW_NUMBER() over (partition by fk_person1 order by DBT_VALID_TO DESC NULLS last) as rank
  from {{ ref("fk_modia__person_oversikt_scd") }}
  where DBT_VALID_TO is null or
          ( TO_CHAR(DBT_VALID_TO, 'YYYYMM') >= TO_CHAR(TO_DATE('{{var("last_mnd_start")}}','YYYY-MM-DD'), 'YYYYMM') or
            TO_CHAR(DBT_VALID_TO, 'YYYYMM') <= TO_CHAR(TO_DATE('{{var("running_mnd")}}','YYYY-MM-DD'), 'YYYYMM'))

),

sykefravar_med_enhet as (
  select sykefravar_med_flagg.*,oversikt_status_scd.TILDELT_ENHET
  from sykefravar_med_flagg
  LEFT JOIN oversikt_status_scd  ON sykefravar_med_flagg.fk_person1 = oversikt_status_scd.fk_person1
  where oversikt_status_scd.rank = 1
),

dim_tid as (
  select *
  FROM {{ ref("felles_dt_p__dim_tid") }}
),

sykefravar_med_tid as (
  select sykefravar_med_enhet.*, dim_tid.pk_dim_tid as FK_DIM_TID_SF_START_DATO
  from sykefravar_med_enhet
  left join dim_tid on dim_tid.pk_dim_tid = to_number(to_char(sykefravar_med_enhet.siste_sykefravar_startdato, 'YYYYMMDD'))

),

dim_organisasjon as (
  select *
  FROM {{ ref("felles_dt_p__dim_organisasjon") }}
),

sykefravar_med_organisasjon as (
  select sykefravar_med_tid.*, dim_organisasjon.PK_DIM_ORGANISASJON, dim_organisasjon.GYLDIG_FRA_DATO, dim_organisasjon.GYLDIG_TIL_DATO
  from sykefravar_med_tid
  left join dim_organisasjon on dim_organisasjon.NAV_ENHET_KODE = sykefravar_med_tid.TILDELT_ENHET
    where dim_organisasjon.GYLDIG_FRA_DATO <= SISTVURDERT AND GYLDIG_TIL_DATO >= SISTVURDERT and
          DIM_NIVAA = 6 and dim_organisasjon.GYLDIG_FLAGG = 1
),

dim_person as (
  select *
  from {{ ref("felles_dt_person__dim_person1")}}
),

sykefravar_med_person as (
  select
    sykefravar_med_organisasjon.*,
    dim_person.fk_dim_geografi_bosted,
    TRUNC(MONTHS_BETWEEN(siste_dag_i_mnd, dim_person.fodt_dato)/12) as alder
  from sykefravar_med_organisasjon
  left join dim_person on dim_person.fk_person1 = sykefravar_med_organisasjon.fk_person1
   and DIM_PERSON.GYLDIG_FLAGG = 1
),

dim_alder as (
  select
    *
  from {{ ref("felles_dt_p__dim_alder") }}
),

sykefravar_med_alder as (
  select
    sykefravar_med_person.*,
    dim_alder.pk_dim_alder as fk_dim_alder
  from sykefravar_med_person
  left join dim_alder on dim_alder.alder = sykefravar_med_person.alder
  where dim_alder.GYLDIG_FLAGG = 1
),

aatte_uker_flagg as (
  select
      sykefravar_med_alder.*,
      CASE WHEN TRUNC(SISTVURDERT, 'DD') - TRUNC(SISTE_SYKEFRAVAR_STARTDATO, 'DD') <= 56 AND STATUS = 'UNNTAK' THEN 1 ELSE 0 END as UNNTAK_FOER_8_UKER_FLAGG,
      CASE WHEN TRUNC(SISTVURDERT, 'DD') - TRUNC(SISTE_SYKEFRAVAR_STARTDATO, 'DD') > 56 AND STATUS = 'UNNTAK' THEN 1 ELSE 0 END as UNNTAK_ETTER_8_UKER_FLAGG
  from sykefravar_med_alder
)


SELECT * FROM aatte_uker_flagg

