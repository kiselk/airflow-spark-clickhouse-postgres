DROP TABLE IF EXISTS agg_rates;
CREATE TABLE agg_rates (base String, symbol String, min_rate Float64 CODEC(Gorilla), avg_rate Float64 CODEC(Gorilla), max_rate Float64 CODEC(Gorilla), date Date, timestamp DateTime CODEC(DoubleDelta) ) ENGINE = MergeTree() partition by toYYYYMM(date) order by date;
