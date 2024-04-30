with sporbar_utbetaling as (
  select *  from {{ ref('fk_sykp__utbetaling') }}
),

sporbar_vedtak as (
  select * from  {{ ref('fk_sykp__vedtak') }}
),


--
CREATE OR REPLACE FORCE EDITIONABLE VIEW "DVH_SYKP"."SYKP_UTBETALING_SPORBAR" ("VEDTAK_ID", "PASIENT_FK_PERSON1", "ORGANISASJONSNUMMER", "SOKNAD_ID", "SOKNAD_FOM_DATO", "SOKNAD_TOM_DATO", "SYKM_ID", "UTBETALT_DATO_TRUNC", "UTBETALT_DATO", "MAKSDATO", "FORBRUKTE_SYKEDAGER", "GJENSTAAENDE_SYKEDAGER", "MOTTAKER", "FAGSYSTEM_ID", "UTBETALINGSLINJE_ID", "UTBETALT_FOM_DATO", "UTBETALT_TOM_DATO", "DAGSATS", "GRAD", "STONADSDAGER", "BELOP") AS
  select u.id,
  u.pasient_fk_person1,
  u.organisasjonsnummer,
  d1.dokument_id,
  v.soknad_fom_dato,
  v.soknad_tom_dato,
  d2.dokument_id,
  trunc(u.kafka_mottatt_dato),
  u.kafka_mottatt_dato,
  u.maksdato,
  u.forbrukte_sykedager,
  u.gjenstaaende_sykedager,
  nvl(o.mottaker, o.mottaker_fk_person1),
  o.fagsystem_id,
  l.id,
  l.utbetalt_fom_dato,
  l.utbetalt_tom_dato,
  l.dagsats,
  l.grad,
  l.stonadsdager,
  l.totalbelop
from dvh_sykp.fk_sporbar_utbetaling u
join dvh_sykp.fk_sporbar_vedtak v on v.utbetaling_id = u.utbetaling_id
join dvh_sykp.fk_sporbar_oppdrag o on u.id = o.fk_sporbar_utbetaling_id
join dvh_sykp.fk_sporbar_utbetalingslinje l on l.fk_sporbar_oppdrag_id = o.id
left outer join dvh_sykp.fk_sporbar_vedtak_dokument d1 on d1.fk_sporbar_vedtak_id = v.id and d1.dokument_type = 'Søknad'
left outer join dvh_sykp.fk_sporbar_vedtak_dokument d2 on d2.fk_sporbar_vedtak_id = v.id and d2.dokument_type = 'Sykmelding'
union
select
    u.pk_sporbar_annullert_vedtak_utbetaling,
    u.pasient_fk_person1,
    u.organisasjonsnummer,
    u.vedtak_soknad_id,
    u.vedtak_soknad_fom_dato,
    u.vedtak_soknad_tom_dato,
    u.vedtak_sykm_id,
    trunc(u.utbetalt_dato),
    u.utbetalt_dato,
    u.maksdato,
    u.forbrukte_sykedager,
    u.gjenstaaende_sykedager,
    nvl(o.mottaker, o.mottaker_fk_person1),
    o.fagsystem_id,
    l.pk_sporbar_annullert_utbetalingslinje,
    l.utbetalt_fom_dato,
    l.utbetalt_tom_dato,
    l.dagsats,
    l.grad,
    l.stonadsdager,
    l.totalbelop
from dvh_sykp.sporbar_annullert_vedtak_utbetaling u
join dvh_sykp.sporbar_annullert_oppdrag o on o.fk_sporbar_annullert_vedtak_utbetaling = u.pk_sporbar_annullert_vedtak_utbetaling
join dvh_sykp.sporbar_annullert_utbetalingslinje l on l.fk_sporbar_annullert_oppdrag = o.pk_sporbar_annullert_oppdrag;

--