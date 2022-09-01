{{ config(
    tags=["IA_PIA"]
) }}

with source_fak_sykm_sykefravar_tilfelle as (
    select  *  from {{ source ('dmx_poc_sykefravar', 'FAK_SYKM_SYKEFRAVAR_TILFELLE')}}
),


final as (
    select * from source_fak_sykm_sykefravar_tilfelle
    where  extract(year from sykefravar_fra_dato) > 2022
)

select * from final