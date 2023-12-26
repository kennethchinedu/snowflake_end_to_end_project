use role sysadmin;
use warehouse dbt_warehouse;
use schema cricket.clean;


-- Extracting meta data
select 
    meta['data_version']::text as data_version,
    meta['created']::date as created,
    meta['revision']::number as revision
from 
    cricket.raw.match_table;

-- Extracting variant type data information from the info column
select 
    info:match_type_number::int as match_type_number,
    info:match_type:text as match_type,
    info:season::text as season,
    info:team_type::text as team_type,
    info:overs::text as overs,
    info:city::text as city,
    info:venue::text as venue
from 
    cricket.raw.match_table;



-- extracting relevant data we need from the info columns
create or replace transient table cricket.clean.match_table_clean as 
select 
    info:match_type_number::int as match_type_number,
    info:event.name::text as event_name,
    case 
    when 
        info:event.match_number::text is not null then info:event:match_number::text
    when 
        info:event.stage::text is not null then info:event:stage::text 
    else
        'NA'
    end as match_stage,
    --date for the first match
    info:dates[0]::date as event_date,
    date_part('year', info:dates[0]::date) as event_year,
    date_part('month', info:dates[0]::date) as event_month,
    date_part('day', info:dates[0]::date) as event_day,
    info:match_type::text as match_type,
    info:season::text as season,
    info:team_type::text as team_type,
    info:overs::text as overs,
    info:city::text as city,
    info:venue::text as venue,
    info:gender::text as gender,
    info:teams[0]::text as first_team,
    info:teams[1]::text as second_team,
    case 
        when info:outcome.winner is not null then 'Result Declared'
        when info:outcome.result = 'tie' then 'Tie'
        when info:outcome.result = 'no result' then 'No Result'
        else info:outcome.result
    end as match_result,
    case 
        when info:outcome.winner is not null then info:outcome.winner
        else 'NA'
    end as winner,

    info:toss.winner::text as toss_winner,
    initcap(info:toss.decision::text) as toss_decision,

    -- other important data to monitor table 
    stg_file_name,
    stg_file_row_number,
    stg_file_hashkey,
    stg_modified_ts
    from 
        cricket.raw.match_table;


-- confirming data
select * from match_table_clean;




create or replace table cricket.clean.players_clean_tbl as 
select 
    raw.info:match_type_number::int as match_type_number,
    p.key::text as country,
    team.value::text as player_name,
    stg_file_name,
    stg_file_row_number,
    stg_file_hashkey,
    stg_modified_ts
from cricket.raw.match_table raw,
lateral flatten (input => raw.info:players) p,
lateral flatten (input => p.value) team;
    

-- setting not null values
alter table cricket.clean.players_clean_tbl
modify column match_type_number set not null;

alter table cricket.clean.players_clean_tbl
modify column country set not null;

alter table cricket.clean.players_clean_tbl
modify column player_name set not null;

-- Adding a unique constraint to the match_type_number column
alter table cricket.clean.match_table_clean
add constraint unique_match_type_number unique (match_type_number);



-- creating table relationship 
alter table cricket.clean.players_clean_tbl
add constraint fk_match_id 
foreign key (match_type_number)
references cricket.clean.match_table_clean (match_type_number);
    
-- validating data
select * from players_clean_tbl;



-- creating innings table
create or replace transient table cricket.clean.delivery_clean_tbl as
select
    m.info:match_type_number::int as match_type_number,
    i.value:team::text as team_name,
    o.value:over::int as over,
    d.value:bowler::text as bowler,
    d.value:batter::text as batter,
    d.value:non_stricker::text as non_stricker,
    d.value:runs.batter::text as runs,
    d.value:runs.extras::text as extras,
    d.value:runs.total::text as total,
    e.key::text as extra_type,
    e.value::number as extra_runs,
    w.value:player_out::text as player_out,
    w.value:kind::text as player_out_kind,
    w.value:fielders::variant as player_out_fielders,
    m.stg_file_name,
    m.stg_file_row_number,
    m.stg_file_hashkey,
    m.stg_modified_ts
from cricket.raw.match_table m,
lateral flatten (input => m.innings) i,
lateral flatten (input => i.value:overs) o,
lateral flatten (input => o.value:deliveries) d,
lateral flatten (input => d.value:extras, outer => True) e, 
lateral flatten (input => d.value:wickets, outer => True) w
;

-- confirming data
select * from cricket.clean.delivery_clean_tbl;




