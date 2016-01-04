--add database catalog engines: engine_id, name, host, port, connection_properties
delete from catalog.engines;
insert into catalog.engines values(0,'postgres1','localhost',5431,'engine for bigdawg catalog, schema and mimic2 data');
insert into catalog.engines values(1,'postgres2','localhost',5430,'main engine for mimic2_copy data');
--add catalog.databases inside the database instances: dbid, engine_id, name, userid, password
delete from catalog.databases;
insert into catalog.databases values(0,0,'bigdawg_catalog','postgres','test');
insert into catalog.databases values(1,0,'bigdawg_schemas','postgres','test');
insert into catalog.databases values(2,0,'mimic2','pguser','test');
insert into catalog.databases values(3,1,'mimic2_copy','pguser','test');
