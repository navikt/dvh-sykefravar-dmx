
    select
    pk_dim_tid as unique_field,
    count(*) as n_records
from dvh_syfo.stg_dmx_data_dim_tid
where pk_dim_tid is not null
group by pk_dim_tid
having count(*) > 1
