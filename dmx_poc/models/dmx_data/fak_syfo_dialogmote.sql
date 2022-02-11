with isdialogmote as
(select * from {{ source('dmx_pox_dialogmote', 'FK_ISDIALOGMOTE_DM2') }}
)
select *
from isdialogmote