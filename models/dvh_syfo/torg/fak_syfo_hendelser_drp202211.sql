with syfo210 as (
    SELECT * FROM  {{ ref('mk_syfo210_m_flagg_og_perioder') }}
        UNPIVOT exCLUDE NULLS(
            periode FOR periode_type IN (
                periode_dm AS 'dm_periode',
                periode_kandidat AS 'kandidat_periode'
            )
        )
),

final as (
    select
        fk_person1,
        tilfelle_startdato,
        dialogmote_tidspunkt,
        dm_innen_26u,
        decode(periode_type, 'kandidat_periode', 0, dm_etter_26u) as dm_etter_26u, -- Settes til 0 for å forhindre at dm_etter_26u blir telt i kandidatperiode
        decode(periode_type, 'kandidat_periode',1,0) as andre_aktuelle,
        periode,
        TO_NUMBER(
            CONCAT(periode, '003')
        ) AS fk_dim_tid__periode
    from syfo210
)

select * from final