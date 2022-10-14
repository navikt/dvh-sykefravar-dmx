

WITH fak_syfo_dm_begge AS (
    SELECT * FROM {{ref('fak_syfo_dt_dm_begge')}}
),

base_kandidater AS (
    SELECT * FROM {{ref('base_modia__kandidater_fk1')}}
),

final AS (
    SELECT fak_syfo_dm_begge.*,
           base_kandidater.*
   FROM fak_syfo_dm_begge
    outer JOIN base_kandidater
    ON    fak_syfo_dm_begge.F_FK_PERSON1 = base_kandidater.fk_person1
    where 

)


SELECt final.* FROM final