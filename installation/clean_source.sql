BEGIN;
DROP TRIGGER region_crud;
DROP FUNCTION change_region;
DROP TABLE region_new;
DROP TABLE region_delete;
DROP TABLE region_update;
DROP TABLE region_old;
COMMIT;
