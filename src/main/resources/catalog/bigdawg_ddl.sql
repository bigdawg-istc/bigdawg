CREATE SCHEMA IF NOT EXISTS catalog;

CREATE TABLE IF NOT EXISTS catalog.islands (
       iid integer PRIMARY KEY,
       scope_name varchar(15), -- e.g., RELATIONAL, ARRAY                                                                                                                                 
       access_method varchar(30) -- how do we bring up the parser to validate statements in the island language?                                                                   
); 

-- underlying database engines                                                                                                                                                             
CREATE TABLE IF NOT EXISTS catalog.engines (
       eid integer PRIMARY KEY,
       name varchar(15),
       host varchar(40),
       port integer,
       connection_properties varchar(100) -- currently hold scidb bin path
);

CREATE UNIQUE INDEX hostPort on catalog.engines (host,port);

CREATE TABLE IF NOT EXISTS catalog.shims (
       shim_id integer PRIMARY KEY,
       island_id integer REFERENCES catalog.islands(iid),
       engine_id integer REFERENCES catalog.engines(eid),
       access_method varchar(30)
);


CREATE TABLE IF NOT EXISTS catalog.casts (
       src_eid integer REFERENCES catalog.engines(eid),
       dst_eid integer REFERENCES catalog.engines(eid),
       access_method varchar(30),
       PRIMARY KEY(src_eid,dst_eid)
);

-- sometimes once we get to an engine, we may have to connect to a specific db created in it                                                                                        
CREATE TABLE IF NOT EXISTS catalog.databases (
       dbid integer PRIMARY KEY,
       engine_id integer REFERENCES catalog.engines(eid),
       name varchar(15), 
       userid varchar(15),
       password varchar(15) -- may be hash of pwd
);

CREATE TABLE IF NOT EXISTS catalog.scidbbinpaths (
    eid integer PRIMARY KEY,
    bin_path varchar(100)
);

-- we need to model objects in terms of where they were created and their present storage site                                                                                      
-- e.g., if we created an array with dimensions X,Y and then we migrate it over to psql, we don't want to lose its initial dimensions                                               
CREATE TABLE IF NOT EXISTS catalog.objects (
       oid integer PRIMARY KEY,
       name varchar(50), -- name of the object
       fields varchar(800), -- csv of the field names, e.g. "dbid,\"engine id\",name,userid,password"
       logical_db integer REFERENCES catalog.databases(dbid), -- how was the object created                                                                                               
       physical_db integer REFERENCES catalog.databases(dbid) -- where is it located now?                                                                                                 
);
