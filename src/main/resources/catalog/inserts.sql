--add database catalog engines: engine_id, name, host, port, connection_properties
insert into catalog.engines values(0,'postgres1','localhost',5431,'engine for bigdawg catalog, schema and mimic2 data');
insert into catalog.engines values(1,'postgres2','localhost',5430,'main engine for mimic2_copy data');

insert into catalog.engines values(2,'scidb','localhost',1239,'scidb database');

--add catalog.databases inside the database instances: dbid, engine_id, name, userid, password
insert into catalog.databases values(0,0,'bigdawg_catalog','postgres','test');
insert into catalog.databases values(3,0,'bigdawg_schemas','postgres','test');
insert into catalog.databases values(1,0,'mimic2','pguser','test');
insert into catalog.databases values(2,1,'mimic2_copy','pguser','test');

-- catalog.islands
-- iid	scope_name	access_method
insert into catalog.islands values (0, 'RELATIONAL', 'Everything written in PSQL');

-- catalog.shims
-- shim_id	island_id	engine_id	access_method	
insert into catalog.shims values (0, 0, 0, 'N/A');

-- catalog.scidbbinapath
-- binary path to scidb utilities: csv2scidb, iquery, etc.
insert into catalog.scidbbinpaths values (2,'/opt/scidb/14.12/bin/');

-- catalog.objects
-- oid	name	fields	logical_db	physical_db
insert into catalog.objects values (0, 'demographics', 'patient_id,birth_year,gender,race,ethnicity,insurance,zip', 1, 1);
insert into catalog.objects values (1, 'demographics', 'patient_id,birth_year,gender,race,ethnicity,insurance,zip', 1, 0);
insert into catalog.objects values (2, 'diagnoses', 'patient_id,timestamp_,visit_no,type_,encounter_id,diag_src,icd9,primary_', 1, 0);
insert into catalog.objects values (3, 'vitals', 'patient_id,height_timestamp,height_visit_no,height,height_units,weight_timestamp,weight_visit_no,weight,weight_units,bmi_timestamp,bmi_visit_no,bmi,bmi_units,pulse,systolic,diastolic,bp_method)', 1, 1);
insert into catalog.objects values (4, 'vitals', 'patient_id,height_timestamp,height_visit_no,height,height_units,weight_timestamp,weight_visit_no,weight,weight_units,bmi_timestamp,bmi_visit_no,bmi,bmi_units,pulse,systolic,diastolic,bp_method)', 1, 0);
insert into catalog.objects values (5, 'labs', 'patient_id,timestamp_,test_name,value_,unit,value_low,value_high', 1, 0);
insert into catalog.objects values (6, 'medications', 'patient_id,timestamp_,medication,dosage,route', 1, 1);
insert into catalog.objects values (7, 'site', 'id', 1, 1);
insert into catalog.objects values (8, 'mimic2v26.d_patients', 'id,lastname,firstname', 1, 1);
insert into catalog.objects values (9, 'ailment', 'id,disease_name', 2, 2);

-- catalog.casts
-- src_eid	dst_eid	access_method


