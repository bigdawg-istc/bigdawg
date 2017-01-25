-- THESE NEED TO BE APPLIED TO bigdawg_catalog TO ALLOW EXECUTION FOR ACCUMULO
insert into catalog.islands values (2, 'TEXT', 'JSON');
insert into catalog.engines values (4, 'saw ZooKeeper', '128.30.76.163', 2181, 'Accumulo 1.6');
insert into catalog.shims values (4, 2, 4, 'N/A');
ALTER TABLE catalog.databases ALTER COLUMN password TYPE varchar(30);
insert into catalog.databases values (7, 4, 'accumulo', 'bigdawg', 'bigdawg');
insert into catalog.objects values (47, 'pythontest', '', 7, 7);