{{ config(
    materialized='view',
    post_hook= ["grant READ ON dvh_syfo.fak_dialogmote to DVH_SYK_DBT"]
)}}


with dialogmote_dm_tidspunkt as (
  select * from {{ref('fak_dialogmote')}}
),

dialogmote_hendelse_tidspunkt as (
  select * from {{ ref('fak_dialogmote_hendelse_tidspunkt') }}
),

final as (
  select
    a.*,
    b.dialogmote2_avholdt_dato as dm2_referat_ferdigstilt,
    b.dialogmote3_avholdt_dato as dm3_referat_ferdigstilt,
    b.dialogmote4_avholdt_dato as dm4_referat_ferdigstilt,
    b.dialogmote5_avholdt_dato as dm5_referat_ferdigstilt,
    b.dialogmote6_avholdt_dato as dm6_referat_ferdigstilt,
    b.dialogmote7_avholdt_dato as dm7_referat_ferdigstilt
  from dialogmote_dm_tidspunkt a
  left join dialogmote_hendelse_tidspunkt b
  on a.fk_person1=b.fk_person1 and a.tilfelle_startdato=b.tilfelle_startdato
)

select * from final