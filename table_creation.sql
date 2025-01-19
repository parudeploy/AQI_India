-- change context
use role sysadmin;
use schema dev_db.stage_sch;
use warehouse adhoc_wh;


-- create an internal stage and enable directory service
create stage if not exists airquality_stg
directory = ( enable = true)
comment = 'all the air quality raw data will store in this internal stage location';


 -- create file format to process the JSON file
  create file format if not exists json_file_format 
      type = 'JSON'
      compression = 'AUTO' 
      comment = 'this is json file format object';


  show stages;
  list @airquality_stg;

  -- load the data that has been downloaded manually
  -- run the list command to check it

  -- level-1
select 
    * 
from 
    @dev_db.stage_sch.airquality_stg
    (file_format => JSON_FILE_FORMAT) t;

  -- JSON file analysis using json editor
  -- level-2
    select 
        Try_TO_TIMESTAMP(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as index_record_ts,
        t.$1,
        t.$1:total::int as record_count,
        t.$1:version::text as json_version  
    from @dev_db.stage_sch.airquality_stg
    (file_format => JSON_FILE_FORMAT) t;

-- level3
select 
    Try_TO_TIMESTAMP(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as index_record_ts,
    t.$1,
    t.$1:total::int as record_count,
    t.$1:version::text as json_version,
    -- meta data information
    metadata$filename as _stg_file_name,
    metadata$FILE_LAST_MODIFIED as _stg_file_load_ts,
    metadata$FILE_CONTENT_KEY as _stg_file_md5,
    current_timestamp() as _copy_data_ts

from @dev_db.stage_sch.airquality_stg
(file_format => JSON_FILE_FORMAT) t;

-- @todo interview tag...
-- could you explain the metadata properties
-- why should you use metadata properties
-- how these metadata properties helps you 
-- the table naming convention while building 
-- 16Mb limitations

  
-- creating a raw table to have air quality data
create or replace transient table raw_airquality (
    record_id int primary key autoincrement,
    index_record_ts timestamp not null,
    json_data variant not null,
    record_count number not null default 0,
    json_version text not null,
    -- audit columns for debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp()
);

-- should you create transient table or permanent table? if so why?
-- how the standard table cost more with fail safe concept?

-- copy command



-- following copy command
create or replace task copy_air_quality_data
    warehouse = load_wh
    schedule = 'USING CRON 0 * * * * Asia/Kolkata'
as
copy into raw_airquality (index_record_ts,json_data,record_count,json_version,_stg_file_name,_stg_file_load_ts,_stg_file_md5,_copy_data_ts) from 
(
    select 
        Try_TO_TIMESTAMP(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as index_record_ts,
        t.$1,
        t.$1:total::int as record_count,
        t.$1:version::text as json_version,
        metadata$filename as _stg_file_name,
        metadata$FILE_LAST_MODIFIED as _stg_file_load_ts,
        metadata$FILE_CONTENT_KEY as _stg_file_md5,
        current_timestamp() as _copy_data_ts
            
   from @dev_db.stage_sch.airquality_stg as t
)
file_format = (format_name = 'dev_db.stage_sch.JSON_FILE_FORMAT') 
ON_ERROR = ABORT_STATEMENT; 


use role accountadmin;
grant execute task, execute managed task on account to role sysadmin;
use role sysadmin;

show task copy_air_quality_data

alter task dev_db.stage_sch.copy_air_quality_data resume;

-- check the data
select *
    from raw_airquality
    limit 10;

-- select with ranking

--latest one
select 
    index_record_ts,record_count,json_version,_stg_file_name,_stg_file_load_ts,_stg_file_md5 ,_copy_data_ts,
    row_number() over (partition by index_record_ts order by _stg_file_load_ts desc) as latest_file_rank
from raw_airquality 
order by index_record_ts desc
limit 100;

--asecending one
select 
    record_id,index_record_ts,record_count,json_version,_stg_file_name,_stg_file_load_ts,_stg_file_md5 ,_copy_data_ts,
    row_number() over (partition by index_record_ts order by _stg_file_load_ts) as latest_file_rank
from raw_airquality 
order by record_id,index_record_ts desc
limit 100;

select count(*) from raw_airquality