with

    -----------------------------------------MT4 part------------------------------------------------------------

    -- we need to skip trades, which id's are marked in a separate table with a bitmask - first bit is 1
    mt4_skipped_trades as (select ticket
                           from mt4.marked_trades
                           where type:: bit(1) = '1'),
    -- filter out table of deals by skipping trades with a bitmask, get only records opening records
    mt4_useful_trades as (select t.*
                          from mt4.trades t
                                   left outer join mt4_skipped_trades s
                                                   on t.ticket = s.ticket),
    -- divide our entire dataset of trades into windows of 30 seconds. time_window is our window index now
    mt4_windowed as (select login,
                            ticket,
                            cmd,
                            floor((extract('epoch' from open_time) / 30))
                                as time_window
                     from mt4_useful_trades),
    -- count trades for every user within the same time frame
    mt4_trades_counted as (select *
                                , count(ticket) over (partition by time_window, login) as same_window_count
                           from mt4_windowed),
    -- get only trades for users with more than 10 trades in the same window
    -- count how many such users are in the same frame
    mt4_users_counted as (select *
                               , dense_rank() over (partition by time_window order by login)
                                     + dense_rank() over (partition by time_window order by login desc) -
                                 1 as users_count
                          from mt4_trades_counted
                          where same_window_count
                                    > 10),
    -- if there is only one suspicious user in one time frame - we ignore this time_window.
    -- need only those frames, which contain at least 2 users for pairing
    mt4_more_than_one_user as (select *
                               from mt4_users_counted
                               where users_count
                                         > 1),
    -- join our trades on themselves on several conditions
    -- 1) time frame is the same
    -- 2) not the same user
    -- 3) trades are opposite in directions e.g. buy vs sell
    mt4_crossed as (select a.login as left, b.login as right
                    from mt4_more_than_one_user a
                             join mt4_more_than_one_user b
                                  on
                                      a.time_window = b.time_window and a.login != b.login and a.cmd != b.cmd
                    where a.login > b.login
                    group by a.login, b.login
                    order by a.login, b.login),

    -----------------------------------------MT5 part------------------------------------------------------------

    -- we need to skip trades, which id's are marked in a separate table with a bitmask - first bit is 1
    mt5_skipped_trades as (select positionid
                           from mt5.marked_trades
                           where type :: bit(1) = '1'),
    -- filter out table of deals by skipping trades with a bitmask, get only records opening records
    mt5_opens as (select d.login, d.positionid, d.action as cmd, d.time as open_time
                  from mt5.deals d
                           left outer join mt5_skipped_trades s
                                           on d.positionid = s.positionid
                  where entry = 0),
    -- divide our entire dataset of trades into windows of 30 seconds. time_window is our window index now
    mt5_windowed as (select login,
                            positionid,
                            cmd,
                            floor((extract('epoch' from open_time) / 30))
                                as time_window
                     from mt5_opens),
    -- count trades for every user within the same time frame
    mt5_trades_counted as (select *
                                , count(positionid) over (partition by time_window, login) as same_window_count
                           from mt5_windowed),
    -- get only trades for users with more than 10 trades in the same window
    -- count how many such users are in the same frame
    mt5_users_counted as (select *
                               , dense_rank() over (partition by time_window order by login)
                                     + dense_rank() over (partition by time_window order by login desc) -
                                 1 as users_count
                          from mt5_trades_counted
                          where same_window_count
                                    > 10),
    -- if there is only one suspicious user in one time frame - we ignore this time_window.
    -- need only those frames, which contain at least 2 users for pairing
    mt5_more_than_one_user as (select *
                               from mt5_users_counted
                               where users_count
                                         > 1),
    -- join our trades on themselves on several conditions
    -- 1) time frame is the same
    -- 2) not the same user
    -- 3) trades are opposite in directions e.g. buy vs sell
    -- removes reversed pairs by their 'age', assuming left has lesser integer than right
    -- use some grouping and sorting
    mt5_crossed as (select a.login as left, b.login as right
                    from mt5_more_than_one_user a
                             join mt5_more_than_one_user b
                                  on
                                      a.time_window = b.time_window and a.login != b.login and a.cmd != b.cmd
                    where a.login > b.login

                    group by a.login, b.login
                    order by a.login, b.login)

-- now get the results for MT4 + MT5
select *
from mt4_crossed mt4
union
select *
from mt5_crossed mt5
