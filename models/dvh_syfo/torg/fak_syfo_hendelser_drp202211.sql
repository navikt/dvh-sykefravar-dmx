with syfo210 as
(SELECT * FROM  {{ ref('mk_syfo210_m_flagg_og_perioder') }}
UNPIVOT exCLUDE NULLS(
    periode
    FOR periode_type
    IN (
        periode_dm AS 'dm_periode',
        periode_kandidat AS 'kandidat_periode'
    )
)),
final as (
select
person_ident,
tilfelle_startdato,
dialogmote_tidspunkt,
dm_innen_26u,
dm_etter_26u,
decode(periode_type, 'kandidat_periode',1,0) as andre_aktuelle,
periode
 from syfo210
 )
 select * from final