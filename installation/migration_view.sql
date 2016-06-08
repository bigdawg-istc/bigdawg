BEGIN;
ALTER TABLE region RENAME TO region_old;

CREATE TABLE region_new AS SELECT * FROM region_old LIMIT 0;
CREATE TABLE region_update AS SELECT * FROM region_old LIMIT 0;
CREATE TABLE region_delete AS SELECT * FROM region_old LIMIT 0;

CREATE OR REPLACE VIEW region AS
       SELECT * FROM region_new UNION 
       (SELECT * FROM region_old EXCEPT 
       (SELECT * FROM region_delete UNION 
       SELECT * FROM region_update));

CREATE OR REPLACE FUNCTION change_region() 
RETURNS TRIGGER AS $$
    BEGIN
        --
        -- Perform the required operation on the table
        --
        IF (TG_OP = 'DELETE') THEN
            DELETE FROM region_new WHERE 
	    (r_regionkey,r_name,r_comment) = 
	    (OLD.r_regionkey,OLD.r_name,OLD.r_comment);
	    INSERT INTO region_delete VALUES(OLD.*);
            IF NOT FOUND THEN RETURN NULL; END IF;
            RETURN OLD;
        ELSIF (TG_OP = 'UPDATE') THEN
	    INSERT INTO region_new VALUES(NEW.*);
	    INSERT INTO region_update VALUES(OLD.*);	
            IF NOT FOUND THEN RETURN NULL; END IF;
            RETURN NEW;
        ELSIF (TG_OP = 'INSERT') THEN
            INSERT INTO region_new VALUES(NEW.*);
            RETURN NEW;
        END IF;
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER region_crud
INSTEAD OF INSERT OR UPDATE OR DELETE ON region
    FOR EACH ROW EXECUTE PROCEDURE change_region();

COMMIT;

