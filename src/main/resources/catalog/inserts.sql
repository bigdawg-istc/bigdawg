--add database catalog engines: engine_id, name, host, port, connection_properties
delete from catalog.engines;
insert into catalog.engines values(1,'postgres1','localhost',5432,'engine for bigdawg catalog data');
insert into catalog.engines values(2,'postgres2','localhost',5431,'main engine for mimic2 data');
--add catalog.databases inside the database instances: dbid, engine_id, name, userid, password
delete from catalog.databases;
insert into catalog.databases values(1,1,'bigdawg_catalog','postgres','test');
insert into catalog.databases values(2,1,'mimic2','pguser','test');
insert into catalog.databases values(3,2,'mimic2_copy','pguser','test');
