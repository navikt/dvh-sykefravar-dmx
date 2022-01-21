
with stg_oppfolging as (
    select  * from {{ ref ('stg_dmx_data_fs_dialogmote_dmx') }}
),

fak_oppfolging as 
(
   select  * from {{ ref ('stg_dmx_data_fs_fak_sf_oppfolging') }} 
),

final fak_oppfolging_tot 
(
    select * fom stg_oppfolging

    left join fak_oppfolging (stg_oppfolging.FK_PERSON1 = fak_oppfolging.FK_DIM_PERSON)

)


select * from final