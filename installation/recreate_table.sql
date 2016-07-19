-- migrate: region_old
-- 
-- finally after migration of the biggest table:
-- migrate: region_new, region_delete, region_update

BEGIN;
DELETE FROM region_old WHERE (r_regionkey,r_name,r_comment) IN (SELECT * FROM region_delete UNION SELECT * FROM region_update);
INSERT INTO region_old SELECT * FROM region_new;
--DROP VIEW region;
ALTER TABLE region_old RENAME TO region;
DROP TABLE region_new;
DROP TABLE region_delete;
DROP TABLE region_update;
COMMIT;
