WITH aktivitetskrav_mk as (
  SELECT a.*, last_day(LASTET_DATO) as siste_dag_i_mnd
  FROM {{ ref("mk_modia__aktivitetskrav") }} a
),

aktivitetskrav_med_flagg as (
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
    WHEN ARSAKER = 'ANNET' OR ARSAKER1= 'ANNET' OR ARSAKER2 = 'ANNET' THEN 1
    ELSE 0
  END AS annet_flagg,
  CASE
    WHEN ARSAKER = 'INFORMASJON_BEHANDLER' OR ARSAKER1= 'INFORMASJON_BEHANDLER' OR ARSAKER2 = 'INFORMASJON_BEHANDLER' THEN 1
    ELSE 0
  END AS informasjon_behandler_flagg,
  CASE
    WHEN ARSAKER = 'OPPFOLGINGSPLAN_ARBEIDSGIVER' OR ARSAKER1= 'OPPFOLGINGSPLAN_ARBEIDSGIVER' OR ARSAKER2 = 'OPPFOLGINGSPLAN_ARBEIDSGIVER' THEN 1
    ELSE 0
  END AS oppfolgingsplan_arbeidsgiver_flagg,
  CASE
    WHEN ARSAKER NOT IN
    ('FRISKMELDT', 'GRADERT', 'MEDISINSKE_GRUNNER','SJOMENN_UTENRIKS','TILRETTELEGGING_IKKE_MULIG', 'ANNET', 'INFORMASJON_BEHANDLER', 'OPPFOLGINGSPLAN_ARBEIDSGIVER') THEN 1
    ELSE 0
  END AS ukjent_blank_flagg
FROM aktivitetskrav_mk

),

/* Max for å hente siste gjeldende dato for når record var gyldig innenfor en periode (måned).
Brukes for å filtrere ut riktig record siden.
Min for å passe på at vi ikke mister aktivitetskrav med periode tidligere enn vi har data på
i person_oversikt_scd.
*/
person_oversikt_scd as (
  select
    FK_PERSON1 as FK_PERSON1_SCD,
    TILDELT_ENHET,
    DBT_VALID_FROM,
    DBT_VALID_TO,
    max(DBT_VALID_FROM) over(partition by FK_PERSON1, TO_CHAR(DBT_VALID_FROM, 'YYYYMM') ) as max_dbt_valid_from_periode,
    TO_CHAR(min(DBT_VALID_FROM) over (partition by FK_PERSON1), 'YYYYMM') as min_periode_scd
  from {{ ref("fk_modia__person_oversikt_scd") }}
  order by DBT_VALID_TO desc
),

/* Case løser modellering over flere måneder, og sørger for at det for en gitt periode hentes riktig tildelt enhet.
    Uten denne hentes record fra første måned og siste måned. */
aktivitetskrav_med_tildelt_enhet as (
  select
    aktivitetskrav_med_flagg.*,
    person_oversikt_scd.*
  from aktivitetskrav_med_flagg
    LEFT JOIN person_oversikt_scd ON aktivitetskrav_med_flagg.fk_person1 = person_oversikt_scd.fk_person1_scd
),

aktivitetskrav_sett_gyldig_enhet_flagg as (
  select
    aktivitetskrav_med_tildelt_enhet.*,
    case
      when PERIODE <= TO_CHAR(DBT_VALID_TO, 'YYYYMM')
        and PERIODE = TO_CHAR(max_dbt_valid_from_periode, 'YYYYMM')
        and DBT_VALID_FROM = max_dbt_valid_from_periode
        then 1
      when PERIODE >= TO_CHAR(max_dbt_valid_from_periode, 'YYYYMM')
        and TO_CHAR(DBT_VALID_TO, 'YYYYMM') is NULL
        and DBT_VALID_FROM = max_dbt_valid_from_periode
        then 1
      when TILDELT_ENHET is null
        then 1
      when PERIODE < min_periode_scd
        then 1
      else 0
      end as valid_flag
  from aktivitetskrav_med_tildelt_enhet
),

aktivitetskrav_gyldig_enhet as (
  select *
  from aktivitetskrav_sett_gyldig_enhet_flagg
  where valid_flag = 1
),

dim_tid as (
  select *
  FROM {{ ref("felles_dt_p__dim_tid") }}
),

sykefravar_med_tid as (
  select aktivitetskrav_gyldig_enhet.*, dim_tid.pk_dim_tid as FK_DIM_TID_SF_START_DATO
  from aktivitetskrav_gyldig_enhet
  left join dim_tid on dim_tid.pk_dim_tid = to_number(to_char(aktivitetskrav_gyldig_enhet.siste_sykefravar_startdato, 'YYYYMMDD'))

),

sykefravar_med_stoppunkt_tid as (
  select sykefravar_med_tid.*, dim_tid.pk_dim_tid as FK_DIM_PASSERT_8_UKER
  from sykefravar_med_tid
  left join dim_tid on dim_tid.pk_dim_tid = to_number(to_char(sykefravar_med_tid.STOPPUNKTAT, 'YYYYMMDD'))

),

dim_organisasjon as (
  select *
  FROM {{ ref("felles_dt_p__dim_organisasjon") }}
),

sykefravar_med_organisasjon as (
  select sykefravar_med_stoppunkt_tid.*, dim_organisasjon.PK_DIM_ORGANISASJON, dim_organisasjon.GYLDIG_FRA_DATO, dim_organisasjon.GYLDIG_TIL_DATO
  from sykefravar_med_stoppunkt_tid
  left join dim_organisasjon on dim_organisasjon.NAV_ENHET_KODE = sykefravar_med_stoppunkt_tid.TILDELT_ENHET
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

--TODO sjekk at stoppunktat stemmer ca overens med aatte uker flagg