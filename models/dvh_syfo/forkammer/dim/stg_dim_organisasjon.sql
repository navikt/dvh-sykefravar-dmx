WITH source_dim_organisasjon AS (
  SELECT * FROM {{ source('dmx_pox_oppfolging', 'DIM_ORG') }}
),

final AS (
  SELECT * FROM source_dim_organisasjon
)

SELECT * FROM final

/*
dette er en tekst dette er en test
*/

/* da er vi alle koblet sammen
ja!

*/
