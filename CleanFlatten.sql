use role sysadmin;
use schema dev_db.consumption_sch;
use warehouse adhoc_wh;

-- select * from clean_flatten_aqi_dt where
-- country = 'India' and state ='KA' and station like 'Silk%'
-- order by index_record_ts

CREATE OR REPLACE FUNCTION prominent_index(
    pm25 NUMBER, 
    pm10 NUMBER, 
    so2 NUMBER, 
    no2 NUMBER, 
    nh3 NUMBER, 
    co NUMBER, 
    o3 NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    CASE 
        WHEN COALESCE(pm25, 0) >= GREATEST(
            COALESCE(pm10, 0), 
            COALESCE(so2, 0), 
            COALESCE(no2, 0), 
            COALESCE(nh3, 0), 
            COALESCE(co, 0), 
            COALESCE(o3, 0)
        ) THEN 'PM25'
        
        WHEN COALESCE(pm10, 0) >= GREATEST(
            COALESCE(pm25, 0), 
            COALESCE(so2, 0), 
            COALESCE(no2, 0), 
            COALESCE(nh3, 0), 
            COALESCE(co, 0), 
            COALESCE(o3, 0)
        ) THEN 'PM10'
        
        WHEN COALESCE(so2, 0) >= GREATEST(COALESCE(pm25, 0),COALESCE(pm10, 0),COALESCE(no2, 0),COALESCE(nh3, 0),COALESCE(co, 0),COALESCE(o3, 0)
        ) THEN 'SO2'
        
        WHEN COALESCE(no2, 0) >= GREATEST(
            COALESCE(pm25, 0), 
            COALESCE(pm10, 0), 
            COALESCE(so2, 0), 
            COALESCE(nh3, 0), 
            COALESCE(co, 0), 
            COALESCE(o3, 0)
        ) THEN 'NO2'
        
        WHEN COALESCE(nh3, 0) >= GREATEST(
            COALESCE(pm25, 0), 
            COALESCE(pm10, 0), 
            COALESCE(so2, 0), 
            COALESCE(no2, 0), 
            COALESCE(co, 0), 
            COALESCE(o3, 0)
        ) THEN 'NH3'
        
        WHEN COALESCE(co, 0) >= GREATEST(
            COALESCE(pm25, 0), 
            COALESCE(pm10, 0), 
            COALESCE(so2, 0), 
            COALESCE(no2, 0), 
            COALESCE(nh3, 0), 
            COALESCE(o3, 0)
        ) THEN 'CO'
        
        ELSE 'O3'
    END
$$;
--Testing 
SELECT prominent_index(56,70,12,4,17,47,3) 
SELECT prominent_index(89,70 , 12,	4,	17,	47,	3) 


--creating another function to ensure pm25,pm10 exists in addition non-mandatory pollutantid

CREATE OR REPLACE FUNCTION three_sub_index_criteria(
    pm25 NUMBER, 
    pm10 NUMBER, 
    so2 NUMBER, 
    no2 NUMBER, 
    nh3 NUMBER, 
    co NUMBER, 
    o3 NUMBER
)
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
$$
    CASE 
        WHEN COALESCE(pm25, 0) > 0 OR COALESCE(pm10, 0) > 0 THEN 1
        ELSE 0
    END
    +
    LEAST(
        2,
        (CASE WHEN COALESCE(so2, 0) != 0 THEN 1 ELSE 0 END) +
        (CASE WHEN COALESCE(no2, 0) != 0 THEN 1 ELSE 0 END) +
        (CASE WHEN COALESCE(nh3, 0) != 0 THEN 1 ELSE 0 END) +
        (CASE WHEN COALESCE(co, 0) != 0 THEN 1 ELSE 0 END) +
        (CASE WHEN COALESCE(o3, 0) != 0 THEN 1 ELSE 0 END)
    )
$$;


--Testing second three_sub_index_criteria function
select three_sub_index_criteria (0,0 , 12,	4,	17,	47,	3) 
select three_sub_index_criteria (56,70,12,4,17,47,3) 


--A small function to handle the input value

create or replace function get_int(input_value varchar)
returns number(38,0)
language sql
AS 
$$
        CASE 
            WHEN input_value IS NULL THEN 0
            WHEN input_value = 'NA' THEN 0
            ELSE to_number(input_value) 
        END
$$;

-- Testing
select get_int(NULL)
select get_int('NA')
select get_int(98)


--Create a dynamic tables to calculate the AQI for each rows (wrt to state,location,city,station)

show dynamic tables;

create or replace dynamic table aqi_final_wide_dt
    target_lag='30 min'
    warehouse=ETL_WH
as
select 
        index_record_ts,
        year(index_record_ts) as aqi_year,
        month(index_record_ts) as aqi_month,
        quarter(index_record_ts) as aqi_quarter,
        day(index_record_ts) aqi_day,
        hour(index_record_ts) aqi_hour,
        country,
        state,
        city,
        station,
        latitude,
        longitude,
        pm10_avg,
        pm25_avg,
        so2_avg,
        no2_avg,
        nh3_avg,
        co_avg,
        o3_avg,
        prominent_index(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)as prominent_pollutant,
        case
        when 
        three_sub_index_criteria(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg) > 2 
        then greatest (pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)
        else 0
        end
        as aqi
    from dev_db.transform_sch.clean_flatten_aqi_dt



    --Testing dynamic table .Now aqi is loaded for each of 11K record with logics whichever we have created

    select * from aqi_final_wide_dt where aqi=0  --792 records have AQI as 0
