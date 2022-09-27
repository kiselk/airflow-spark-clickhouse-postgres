with

    -----------------------------------------MT4 part------------------------------------------------------------

    -- we need to skip trades, which id's are marked in a separate table with a bitmask - first bit is 1
    mt4_skipped_trades as (select ticket
                           from mt4.marked_trades
                           where type
                                     :: bit(1) = '1'),
    -- filter out table of deals by skipping trades with a bitmask, get only records opening records
    mt4_useful_trades as (select t.*
                          from mt4.trades t
                                   left outer join mt4_skipped_trades s
                                                   on t.ticket = s.ticket),
    -- get time difference for open and close timestamps, get only those, which happened within 1 minute
    mt4_minute_deals as (select login, ticket, EXTRACT(EPOCH FROM (close_time - open_time)) as diff
                         from mt4_useful_trades
                         where EXTRACT(EPOCH FROM (close_time - open_time))
                                   < 60),
    -- count how many of such deals are there for every login
    mt4_minute_deal_count_by_login as (select login, count(ticket) as minute_deals_count
                                       from mt4_minute_deals
                                       group by login),

    -----------------------------------------MT5 part------------------------------------------------------------

    -- we need to skip trades, which id's are marked in a separate table with a bitmask - first bit is 1
    mt5_skipped_trades as (select positionid
                           from mt5.marked_trades

                           where type :: bit(1) = '1'),
    -- get open deals, filter out  table of deals by skipping trades with a bitmask, get only records opening records
    mt5_opens as (select d.login, d.positionid, d.time t1
                  from mt5.deals d
                           left outer join mt5_skipped_trades s
                                           on d.positionid = s.positionid
                  where entry = 0),
    -- get close deals, filter out  table of deals by skipping trades with a bitmask, get only records opening records
    mt5_closes as (select d.positionid, max(d.time) as t2
                   from mt5.deals d
                            left outer join mt5_skipped_trades s
                                            on d.positionid = s.positionid
                   where entry = 1
                   group by d.positionid),
    -- get time difference for open and close timestamps, get only those, which happened within 1 minute
    mt5_minute_deals as (select o.login, o.positionid, t1, t2, EXTRACT(EPOCH FROM (t2 - t1)) as diff
                         from mt5_opens o
                                  left join mt5_closes c
                                            on o.positionid = c.positionid
                         where EXTRACT(EPOCH FROM (t2 - t1))
                                   < 60),
    -- count how many of such deals are there for every login
    mt5_minute_deal_count_by_login as (select login, count(positionid) as minute_deals_count
                                       from mt5_minute_deals
                                       group by login)

select *
from mt4_minute_deal_count_by_login
union
select *
from mt5_minute_deal_count_by_login
order by minute_deals_count desc