/* Max for å hente siste gjeldende dato for når record var gyldig innenfor en periode (måned).
Brukes for å filtrere ut riktig record siden.
Min for å passe på at vi ikke mister aktivitetskrav med periode tidligere enn vi har data på
i person_oversikt_scd.
*/

WITH aktivitetskrav as (
  select * from {{ ref('mk_aktivitetskrav__siste_tilfelle_periode') }}
),

person_oversikt_scd as (
  select
    fk_person1 as fk_person1_scd,
    tildelt_enhet,
    dbt_valid_from,
    dbt_valid_to,
    max(dbt_valid_from) over(partition by fk_person1, TO_CHAR(dbt_valid_from, 'YYYYMM') ) as max_dbt_valid_from_periode,
    TO_CHAR(min(dbt_valid_from) over (partition by fk_person1), 'YYYYMM') as min_periode_scd
  from {{ ref("fk_modia__person_oversikt_scd") }}
  order by dbt_valid_to desc
),


aktivitetskrav_med_tildelt_enhet as (
  select
    a.*,
    b.*
  from aktivitetskrav a
    LEFT JOIN person_oversikt_scd b ON a.fk_person1 = b.fk_person1_scd
),

/* Case løser modellering over flere måneder, og sørger for at det for en gitt periode hentes riktig tildelt enhet.
    Uten denne hentes record fra første måned og siste måned. */
aktivitetskrav_sett_gyldig_enhet_flagg_steg_1 as (
  select
    a.*,
    case
      when periode <= TO_CHAR(dbt_valid_to, 'YYYYMM')
        and periode = TO_CHAR(max_dbt_valid_from_periode, 'YYYYMM')
        and dbt_valid_from = max_dbt_valid_from_periode
        then 1
      when periode >= TO_CHAR(max_dbt_valid_from_periode, 'YYYYMM')
        and (TO_CHAR(dbt_valid_to, 'YYYYMM') is NULL
          or periode <= TO_CHAR(dbt_valid_to, 'YYYYMM'))
        and dbt_valid_from = max_dbt_valid_from_periode
        then 1
      when tildelt_enhet is null
        then 1
      when periode < min_periode_scd
        and dbt_valid_from = max_dbt_valid_from_periode
        then row_number() over (partition by fk_person1, periode ORDER BY dbt_valid_to nulls first)
      else 0
      end as valid_flag
  from aktivitetskrav_med_tildelt_enhet a
),

aktivitetskrav_sett_gyldig_enhet_flagg_steg_2 as (
  select
    a.*,
    row_number() over (partition by fk_person1, periode ORDER BY dbt_valid_to nulls first) as valid_flag_2
  from aktivitetskrav_sett_gyldig_enhet_flagg_steg_1 a
  where valid_flag = 1
),

aktivitetskrav_gyldig_enhet as (
  select *
  from aktivitetskrav_sett_gyldig_enhet_flagg_steg_2
  where valid_flag_2 = 1
),

final as (
  select
    fk_person1,
    periode,
    status,
    arsaker,
    arsaker1,
    arsaker2,
    sistvurdert,
    stoppunktat,
    siste_tilfelle_startdato,
    tildelt_enhet,
    oppdatert_dato,
    lastet_dato_dbt
  from aktivitetskrav_gyldig_enhet

)


SELECT * FROM final