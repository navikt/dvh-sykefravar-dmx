

with source_fak_sykm_sykefravar_tilfelle as (
    select  *  from {{ source ('sykefravar', 'FAK_SYKM_SYKEFRAVAR_TILFELLE')}}
),


final as (
    select * from source_fak_sykm_sykefravar_tilfelle
)

select * from final