drop view if exists region;
drop table if exists region_new;

drop trigger if exists region_crud;

CREATE OR REPLACE VIEW region AS
       SELECT * FROM region_new UNION (SELECT * FROM region_old EXCEPT (SELECT * FROM region_delete UNION SELECT * FROM region_update));

drop from region_old where 
