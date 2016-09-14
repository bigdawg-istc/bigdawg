;; This buffer is for notes you don't want to save, and for Lisp evaluation.
;; If you want to create a file, visit that file with C-x C-f,
;; then enter the text in that file's own buffer.

alter table "+table+" rename to "+table+"_old;

create table "+table+"_new as select * from "+table+"_old limit 0;
create table "+table+"_update as select * from "+table+"_old limit 0;
create table "+table+"_delete as select * from "+table+"_old limit 0;


CREATE OR REPLACE VIEW "+table+" AS
       SELECT * FROM "+table+"_new UNION (SELECT * FROM "+table+"_old EXCEPT (SELECT * FROM "+table+"_delete UNION SELECT * FROM "+table+"_update));

CREATE OR REPLACE FUNCTION change_"+table+"() RETURNS TRIGGER AS $$
    BEGIN
        --
        -- Perform the required operation on the table
        --
        IF (TG_OP = 'DELETE') THEN
            DELETE FROM "+table+"_new WHERE r_"+table+"key = OLD.r_"+table+"key;
	    INSERT INTO "+table+"_delete VALUES(OLD.*);
            IF NOT FOUND THEN RETURN NULL; END IF;
            RETURN OLD;
        ELSIF (TG_OP = 'UPDATE') THEN
	    INSERT INTO "+table+"_new VALUES(NEW.*);
	    INSERT INTO "+table+"_update VALUES(OLD.*);	
            IF NOT FOUND THEN RETURN NULL; END IF;
            RETURN NEW;
        ELSIF (TG_OP = 'INSERT') THEN
            INSERT INTO "+table+"_new VALUES(NEW.*);
            RETURN NEW;
        END IF;
    END;
$$ LANGUAGE plpgsql;

"CREATE TRIGGER "+table+"_crud INSTEAD OF INSERT OR UPDATE OR DELETE ON "+table+" FOR EACH ROW EXECUTE PROCEDURE change_"+table+"()"



"CREATE OR REPLACE FUNCTION change_"+table+"() 
RETURNS TRIGGER AS $$
    BEGIN
        --
        -- Perform the required operation on the table
        --
        IF (TG_OP = 'DELETE') THEN
            DELETE FROM "+table+"_new WHERE 
	    (r_"+table+"key,r_name,r_comment) = 
	    (OLD.r_"+table+"key,OLD.r_name,OLD.r_comment);
	    INSERT INTO "+table+"_delete VALUES(OLD.*);
            IF NOT FOUND THEN RETURN NULL; END IF;
            RETURN OLD;
        ELSIF (TG_OP = 'UPDATE') THEN
	    INSERT INTO "+table+"_new VALUES(NEW.*);
	    INSERT INTO "+table+"_update VALUES(OLD.*);	
            IF NOT FOUND THEN RETURN NULL; END IF;
            RETURN NEW;
        ELSIF (TG_OP = 'INSERT') THEN
            INSERT INTO "+table+"_new VALUES(NEW.*);
            RETURN NEW;
        END IF;
    END;
$$ LANGUAGE plpgsql"
