with linje_personOppdrag as (
   select *  from {{ ref('fk_sykp__utbetalingslinjeArbeidsgiverOppdrag') }}
),
 linje_arbeidsgiverOppdrag as (
  select * from {{ ref('fk_sykp__utbetalingslinjePersonOppdrag') }}
 ),

linje_totalt AS (
  select * from linje_personOppdrag
  union all
  select * from linje_arbeidsgiverOppdrag
),

final as (
  select
        utbetaling_id,
        oppdragstype,
        fagomraade,
        fagsystem_id,
        mottaker_orgnummer,
        mottaker_fk_person1,
        netto_belop,
        stonadsdager,
        tom,
        fom,
        oppdatert_dato,
        grad,
        dagsats,
        utbetalt_tom_dato,
        utbetalt_fom_dato,
        lastet_dato,
        kildesystem
  from linje_totalt
)

select * from final