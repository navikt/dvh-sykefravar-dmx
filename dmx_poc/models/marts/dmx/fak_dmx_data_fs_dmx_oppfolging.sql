
with stg_dialogmote as (
    select  * from {{ref ('stg_fk_isdialogmote_dm2')}}
),

fak_oppfolging as 
(
   select  * from {{ref('stg_dmx_data_fs_fak_sf_oppfolging')}}
),

final  as
(
    
    select {{get_fields()}} from stg_dialogmote
    left join fak_oppfolging on stg_dialogmote.FK_PERSON1 = fak_oppfolging.FK_DIM_PERSON

)

/* legger inn litt kommentarer
*/

select * from final

/* Den er grei stig */
