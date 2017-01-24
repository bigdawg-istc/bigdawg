-- THESE NEED TO BE APPLIED TO bigdawg_catalog TO ALLOW EXECUTION FOR ACCUMULO
insert into catalog.islands values (2, 'TEXT', 'JSON');
insert into catalog.engines values (4, 'TXE1 ZooKeeper', 'localhost', 2333, 'Accumulo 1.6');
insert into catalog.shims values (4, 2, 4, 'N/A');
ALTER TABLE catalog.databases ALTER COLUMN password TYPE varchar(30);
--insert into catalog.databases values (7, 4, 'classdb55', 'AccumuloUser', ''); -- modify the last entry for classdb55's AccumuloUser password 
--insert into catalog.objects values (47, 'bk0802_oTsampleDegree', '', 7, 7);