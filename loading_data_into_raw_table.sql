use role sysadmin;
use warehouse dbt_warehouse;
use schema cricket.raw;

-- creating table to store raw data from stage 
create or replace transient table cricket.raw.match_table(
    meta object not null,
    info variant not null,
    innings array not null,
    stg_file_name text not null,
    stg_file_row_number int not null,
    stg_file_hashkey text not null,
    stg_modified_ts timestamp not null 
)
comment = "This is a table that stores data from the internal stage"
;

-- copying file into table 
copy into cricket.raw.match_table from 
    (
    select 
    t.$1:meta::object as meta,
    t.$1:info::variant as info,
    t.$1:innings::array as innings,
    --
    metadata$filename,
    metadata$file_row_number,
    metadata$file_content_key,
    metadata$file_last_modified
    from 
        @cricket.land.raw_stage/cricket/json/ (file_format => 'cricket.land.json_format') t
    )
    on_error = continue;

-- checking data
select * from match_table;



