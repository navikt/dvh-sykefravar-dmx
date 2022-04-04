WITH source_fs_dialogmote AS (
  SELECT * FROM {{ source('dmx_pox_dialogmote', 'FK_ISDIALOGMOTE_DM2') }}
),

final AS (
  SELECT * FROM source_fs_dialogmote
)

SELECT * FROM final
