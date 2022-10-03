WITH hendelser as (
  SELECT
    mk_syfo__union.*,
    ROW_NUMBER() OVER(PARTITION BY fk_person1, tilfelle_startdato1, hendelse ORDER BY dialogmote_tidspunkt) AS row_number_dm,
    ROW_NUMBER() OVER(PARTITION BY fk_person1, tilfelle_startdato1, hendelse ORDER BY hendelse_tidspunkt) AS row_number_hendelse,
    ROW_NUMBER() OVER(PARTITION BY fk_person1, tilfelle_startdato1 ORDER BY hendelse_tidspunkt) AS row_number_tilfelle,
    ROW_NUMBER() OVER(PARTITION BY fk_person1 ORDER BY hendelse_tidspunkt) AS row_number_person
  FROM {{ ref('mk_syfo__union') }} mk_syfo__union
)
,
final as (
  SELECT
  hendelser.*
  from hendelser
)
select * from final