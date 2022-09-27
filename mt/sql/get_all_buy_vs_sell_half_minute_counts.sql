delete from link_event_actor;

INSERT INTO link_event_actor (event_id, actor_id)

                SELECT  src.event_id, src.actor_id FROM events src
                left join link_event_actor trg on src.event_id = trg.event_id and src.actor_id = src.actor_id
                where trg.event_id is null