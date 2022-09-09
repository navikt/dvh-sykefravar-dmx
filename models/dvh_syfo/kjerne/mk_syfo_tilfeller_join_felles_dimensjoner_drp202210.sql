with tilfeller
as (
  select * from {{ ref('mk_syfo_tilfeller_join_dialogmote_drp202210') }}
) ,

dim_person
as (
  select
    pk_dim_person,
    fk_person1,
    fk_dim_organisasjon,
    gyldig_fra_dato,
    gyldig_til_dato
    from {{ ref('felles_dt_person__dim_person1') }}

  ),

dim_organisasjon
as (
  select
    pk_dim_organisasjon,
    mapping_node_kode,
    gyldig_fra_dato,
    gyldig_til_dato
  from  {{ ref('stg_dim_organisasjon') }}
  where mapping_node_type = 'ARENAENHET'
),

tilfeller_m_dim_person
as (
  select
    tilfeller.*,
    pk_dim_person as fk_dim_person,
    fk_dim_organisasjon as fk_dim_organisasjon_dim_person
   from tilfeller left join dim_person
   on (tilfeller.fk_person1 = dim_person.fk_person1 and tilfeller.tilfelle_startdato between gyldig_fra_dato and gyldig_til_dato)
),

tilfeller_m_dim_organisasjon
as (
  select
    tilfeller_m_dim_person.*,
    pk_dim_organisasjon as fk_dim_organisasjon_dialogmote
   from tilfeller_m_dim_person left join dim_organisasjon
   on (tilfeller_m_dim_person.enhet_nr = dim_organisasjon.mapping_node_kode and tilfeller_m_dim_person.dialogmote_tidspunkt between gyldig_fra_dato and gyldig_til_dato)
),

final
as (
   select
   tilfeller_m_dim_organisasjon.* ,
  DECODE(fk_dim_organisasjon_dialogmote, NULL, fk_dim_organisasjon_dim_person, fk_dim_organisasjon_dialogmote) as fk_dim_organisasjon
  from tilfeller_m_dim_organisasjon
)

select * from final