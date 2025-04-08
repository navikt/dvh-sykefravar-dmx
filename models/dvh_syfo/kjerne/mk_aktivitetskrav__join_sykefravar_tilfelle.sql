WITH aktivitetskrav as (
  select * from {{ ref("fk_modia__aktivitetskrav") }}

),

sykefravar_tilfeller as(
  select * from {{ ref("stg_fak_sykm_sykefravar_tilfelle") }}

),

aktivitetskrav_med_tilfelle_start_dato as (
  SELECT
    a.*,
    b.sykefravar_fra_dato
  FROM aktivitetskrav a
  LEFT JOIN sykefravar_tilfeller b  ON b.fk_person1 = a.fk_person1
  order by a.FK_PERSON1, sykefravar_fra_dato, kafka_mottatt_dato desc

),

/* Null i sykefravar_fra_dato hvis sykefravær 'under behandling'. Aktivitetskravet kan likevel være opprettet.
Må også inkludere aktivitetskrav med sykefravar_fra_dato etter aktivitetskravet startet og stoppet.
Datoverdi settes lik frist på aktivitetskrav etter 56 dager.
*/

aktivitetskrav_uten_sykefravar_treff as (
  select
    a.*,
    case when (sykefravar_fra_dato is null) or (sykefravar_fra_dato > createdat) or (sykefravar_fra_dato > stoppunktat) then to_date(stoppunktat-56) else sykefravar_fra_dato end as tilfelle_startdato
  from aktivitetskrav_med_tilfelle_start_dato a

),

final as (
  select
    *
  from aktivitetskrav_uten_sykefravar_treff

)

select * from final