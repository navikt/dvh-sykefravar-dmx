{{ config(
    pre_hook=[
      "drop  view {{this}}"
    ]
) }}
WITH fak_syfo_dialogmote_ny AS (
    SELECT * FROM {{ref('fak_syfo_dialogmote_tid')}}
),

fak_arena AS (
    SELECT * FROM {{ref('stg_dmx_data_fak_sf_hendelse_dag')}}
),

final AS 
(
    SELECT fak_syfo_dialogmote_ny.*,
    fak_arena.PK_FAK_SF_HENDELSE_DAG as a_PK_FAK_SF_HENDELSE_DAG,
    fak_arena.FK_DIM_NAERING as a_FK_DIM_NAERING,
    fak_arena.FK_DIM_ORGANISASJON as  a_FK_DIM_ORGANISASJON,
    fak_arena.FK_DIM_PERSON as a_FK_DIM_PERSON,
    fak_arena.FK_DIM_PERSON_BEHOV as a_FK_DIM_PERSON_BEHOV,
    fak_arena.FK_DIM_SF_AARSAK as  a_FK_DIM_SF_AARSAK,
    fak_arena.FK_DIM_SF_HENDELSESTYPE as a_FK_DIM_SF_HENDELSESTYPE,
    fak_arena.KILDESYSTEM as a_KILDESYSTEM
    
    FROM fak_syfo_dialogmote_ny
    full outer join fak_arena ON  fak_syfo_dialogmote_ny.FK_PERSON1 = fak_arena.FK_PERSON1 )

SELECt * FROM final