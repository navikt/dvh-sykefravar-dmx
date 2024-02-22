with

aktivitetskrav as (
  select * from {{ ref('mk_aktivitetskrav__tildelt_enhet_periode') }}
),

dim_tid as (
  select * from {{ ref("felles_dt_p__dim_tid") }}
),

dim_organisasjon as (
  select * from {{ ref("felles_dt_p__dim_organisasjon") }}
),

dim_person as (
  select * from {{ ref("felles_dt_person__dim_person1")}}
),

dim_alder as (
  select * from {{ ref("felles_dt_p__dim_alder") }}
),

aktivitetskrav_med_dim_tid as (
  select
    a.*,
    b.pk_dim_tid as fk_dim_tid_sf_start_dato,
    c.pk_dim_tid as fk_dim_tid_passert_8_uker,
    d.pk_dim_tid as fk_dim_tid_status
  from aktivitetskrav a
  left join dim_tid b on b.pk_dim_tid = to_number(to_char(a.siste_tilfelle_startdato, 'YYYYMMDD'))
  left join dim_tid c on c.pk_dim_tid = to_number(to_char(a.stoppunktat, 'YYYYMMDD'))
  left join dim_tid d on d.pk_dim_tid = to_number(to_char(a.sistvurdert, 'YYYYMMDD'))
),

aktivitetskrav_med_dim_organisasjon as (
  select
    a.*,
    b.pk_dim_organisasjon as fk_dim_organisasjon
  from aktivitetskrav_med_dim_tid a
  left join dim_organisasjon b on b.nav_enhet_kode = a.tildelt_enhet
    where (b.gyldig_fra_dato <= sistvurdert AND gyldig_til_dato >= sistvurdert and
          dim_nivaa = 6 and b.gyldig_flagg = 1) or b.pk_dim_organisasjon is null
),

sykefravar_med_dim_person as (
  select
    a.*,
    b.fk_dim_geografi_bosted,
    TRUNC(MONTHS_BETWEEN(last_day(to_date(a.periode, 'YYYYMM')), b.fodt_dato)/12) as alder
  from aktivitetskrav_med_dim_organisasjon a
  left join dim_person b on b.fk_person1 = a.fk_person1
   and b.gyldig_flagg = 1
),

sykefravar_med_dim_alder as (
  select
    a.*,
    b.pk_dim_alder as fk_dim_alder
  from sykefravar_med_dim_person a
  left join dim_alder b on b.alder = a.alder
  where b.gyldig_flagg = 1
),

final as (
  select
    fk_person1,
    periode,
    arsaker,
    arsaker1,
    arsaker2,
    status,
    sistvurdert,
    stoppunktat,
    oppdatert_dato,
    siste_tilfelle_startdato,
    lastet_dato_dbt,
    fk_dim_tid_sf_start_dato,
    fk_dim_tid_passert_8_uker,
    fk_dim_tid_status,
    fk_dim_organisasjon,
    fk_dim_geografi_bosted,
    fk_dim_alder
  from sykefravar_med_dim_alder
)


SELECT * FROM final