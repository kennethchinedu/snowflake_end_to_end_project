use role sysadmin;


-- creating database and schemas 
create database if not exists cricket;
create or replace schema cricket.land;
create or replace schema cricket.raw;
create or replace schema cricket.clean;
create or replace schema cricket.consumption;

use schema cricket.land;

-- creating file format 
create or replace file format cricket.land.json_format
    type = json 
    null_if = ('\\n', 'null', '')
    strip_outer_array = true 
    comment = 'created json file format';


-- creating stage     
create or replace stage cricket.land.raw_stage;

list @cricket.land.raw_stage;

-- checking data
select 
    l.$1:meta::variant as meta,
    l.$1:info::variant as info,
    l.$1:innings::array as innings,
    metadata$filename as file_name,
    metadata$file_row_number int,
    metadata$file_content_key text,
    metadata$file_last_modified stg_modified 
from 
    @cricket.land.raw_stage/cricket/json/1384407.json (file_format => 'json_format') l;



