DROP TABLE IF EXISTS raw_CSV_rates;
CREATE TABLE raw_CSV_rates (code String,rate String,base String,date String) ENGINE = Memory;
select count(*) as cnt from raw_CSV_rates  ;
