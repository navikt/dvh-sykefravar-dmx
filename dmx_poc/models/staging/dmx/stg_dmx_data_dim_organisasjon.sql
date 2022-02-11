
with source_dim_organisasjon as (
    select  * from {{ source('dmx_pox_oppfolging', 'DIM_ORGANISASJON') }}
),

final as (
    select * from source_dim_organisasjon
)

select * from final

/*
dette er en tekst dette er en test
*/

/* da er vi alle koblet sammen
ja!

*/