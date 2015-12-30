-- add database catalog.engines
insert into catalog.engines values (1,'postgres1','localhost',5431,'engine for bigdawg catalog data and mimic2 data');
insert into catalog.engines values (2,'postgres2','localhost',5430,'main engine for mimic2_copy data');

-- add catalog.databases inside the database instances
insert into catalog.databases values(1,1,'bigdawg','postgres','test');
insert into catalog.databases values(2,1,'mimic2','pguser','test');
insert into catalog.databases values(3,2,'mimic2_copy','pguser','test');
