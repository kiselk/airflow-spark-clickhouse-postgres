--select * from raw_rates

DROP TABLE IF EXISTS agg_rates
CREATE TABLE agg_rates (base String, symbol String, min_rate Float64 CODEC(Gorilla), avg_rate Float64 CODEC(Gorilla), max_rate Float64 CODEC(Gorilla), date Date, timestamp DateTime CODEC(DoubleDelta) ) ENGINE = MergeTree() partition by toYYYYMM(date) order by date;


alter table agg_rates delete  where date in (select max(date) from agg_rates)


insert into agg_rates 
select base, symbol, avg(rate) as avg_rate, min(rate) as min_rate, max(rate) as max_rate,date,NOW()  from raw_rates
where date not in (select distinct date from agg_rates)
group by base, symbol, date;


insert into agg_rates 
select base, symbol, avg(rate) as avg_rate, min(rate) as min_rate, max(rate) as max_rate,date,NOW()  from 

(select base, code as symbol, cast(REPLACE (rate,',','.') as float) as rate, cast(date as Date) as date from raw_CSV_rates
where cast(date as Date) not in (select distinct date from agg_rates) ) as converted
group by base, symbol, date;


select * from agg_rates

select * from raw_CSV_rates