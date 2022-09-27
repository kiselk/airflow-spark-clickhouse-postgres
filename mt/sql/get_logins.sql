
select distinct login from mt5.deals
union
select distinct login  from mt4.trades
order by login
