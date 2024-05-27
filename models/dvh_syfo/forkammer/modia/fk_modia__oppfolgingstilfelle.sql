with src_oppfolgingstilfelle_person as (
  select
      kafka_hash
      , kafka_topic
      , kafka_partisjon
      , kafka_key
      , kafka_offset
      , kafka_mottatt_dato
      , kafka_timestamp
      , kafka_message
      , lastet_dato
      , oppdatert_dato
      , kildesystem
   from {{ source('modia', 'raw_oppfolgingstilfelle') }}
   where kafka_offset > 17000000
)

, dvh_person_ident as (
    select  * from {{ref('felles_dt_person__ident_off_id_til_fk_person1') }}
)

, json_data_person as (
    select
      json_value(oppf.kafka_message, '$.uuid') as uuid
      , json_value(oppf.kafka_message, '$.personIdentNumber') as person_ident
      , json_query(oppf.kafka_message, '$.oppfolgingstilfelleList') as oppfolgingstilfelle_list
      , kafka_partisjon
      , kafka_offset
      , kafka_timestamp
      , kafka_mottatt_dato
      , lastet_dato
      , oppdatert_dato
      , kildesystem
  from src_oppfolgingstilfelle_person oppf
)

, json_data_oppfolgingstilfeller as (
  select
      oppf_person.*
      , case when jt.gradert_at_tilfelle_end = 'true' then 1 else 0 end as gradert_at_tilfelle_end_flagg
      , case when jt.arbeidstaker_at_tilfelle_end = 'true' then 1 else 0 end as arbeidstaker_at_tilfelle_end_flagg
      , jt.syfo_start_dato as syfo_start_dato
      , jt.syfo_slutt_dato as syfo_slutt_dato
      , jt.antall_sykedager as antall_sykedager
      , jt.virksomhetsnummer_list as virksomhetsnummer_list
    from json_data_person oppf_person
    inner join json_table(
        oppf_person.oppfolgingstilfelle_list format json
        , '$[*]' columns(
            gradert_at_tilfelle_end varchar2(255) path '$.gradertAtTilfelleEnd'
            , arbeidstaker_at_tilfelle_end varchar2(255) path '$.arbeidstakerAtTilfelleEnd'
            , syfo_start_dato date path '$.start'
            , syfo_slutt_dato date path '$.end'
            , antall_sykedager number(38) path '$.antallSykedager'
            , virksomhetsnummer_list varchar2(255) format json path '$.virksomhetsnummerList'
        )
    ) jt on 1 = 1
)

, med_fk_person1 as (
  select
    oppf.*
    , dvh_person_ident.fk_person1 as fk_person1
  from json_data_oppfolgingstilfeller oppf
  left join dvh_person_ident on
      oppf.person_ident = dvh_person_ident.off_id
      and oppf.syfo_start_dato between dvh_person_ident.gyldig_fra_dato and dvh_person_ident.gyldig_til_dato
)

, siste_historikk as (
  select
    a.*
  from med_fk_person1 a
  where kafka_timestamp = (select max(b.kafka_timestamp) from med_fk_person1 b where a.fk_person1 = b.fk_person1)
)

, final as (
  select
    uuid
    , fk_person1
    , gradert_at_tilfelle_end_flagg
    , arbeidstaker_at_tilfelle_end_flagg
    , syfo_start_dato
    , syfo_slutt_dato
    , antall_sykedager
    , virksomhetsnummer_list
    , kafka_partisjon
    , kafka_offset
    , kafka_timestamp
    , kafka_mottatt_dato
    , lastet_dato
    , oppdatert_dato
    , kildesystem
  from siste_historikk
)

select * from final