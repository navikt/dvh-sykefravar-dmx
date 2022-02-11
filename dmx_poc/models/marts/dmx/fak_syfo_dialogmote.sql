with isdialogmote as
(
    select *
    from {{ ref('stg_fk_isdialogmote_dm2') }}
),
dim_person1 as
(
    select *
    from {{ ref('stg_dmx_data_dim_person1') }}
),
resultat as
(
    select isdialogmote.*, dim_person1.pk_dim_person as fk_dim_person
    from isdialogmote
    join dim_person1
    on isdialogmote.fk_person1 = dim_person1.fk_person1
    and isdialogmote.dialogmote_tidspunkt between dim_person1.gyldig_fra_dato and dim_person1.gyldig_til_dato
)
select *
from resultat