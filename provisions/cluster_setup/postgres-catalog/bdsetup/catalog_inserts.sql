-- catalog.islands: iid	scope_name	access_method
insert into catalog.islands values (0, 'RELATIONAL', 'PSQL');
insert into catalog.islands values (1, 'ARRAY', 'AFL');
insert into catalog.islands values (2, 'TEXT', 'JSON');

-- catalog.engines: engine_id, name, host, port, connection_properties
insert into catalog.engines values(0,'postgres0','bigdawg-postgres-catalog',5400,'PostgreSQL 9.4.5');
insert into catalog.engines values(1,'postgres1','bigdawg-postgres-data1',5401,'PostgreSQL 9.4.5');
insert into catalog.engines values(2,'postgres2','bigdawg-postgres-data2',5402,'PostgreSQL 9.4.5');
insert into catalog.engines values(3,'scidb_local','bigdawg-scidb-data',1239,'SciDB 14.12');
insert into catalog.engines values (4, 'saw ZooKeeper', '128.30.76.163', 2181, 'Accumulo 1.6');

-- catalog.databases: dbid, engine_id, name, userid, password
insert into catalog.databases values(0,0,'bigdawg_catalog','pguser','test');
insert into catalog.databases values(1,0,'bigdawg_schemas','pguser','test');
insert into catalog.databases values(2,1,'mimic2','pguser','test');
insert into catalog.databases values(3,2,'mimic2_copy','pguser','test');
insert into catalog.databases values(4,0,'tpch','pguser','test');
insert into catalog.databases values(5,1,'tpch','pguser','test');
insert into catalog.databases values(6,3,'scidb_local','scidb','scidb123');
insert into catalog.databases values (7, 4, 'accumulo', 'bigdawg', 'bigdawg');

-- catalog.shims: shim_id	island_id	engine_id	access_method
insert into catalog.shims values (0, 0, 0, 'N/A');
insert into catalog.shims values (1, 0, 1, 'N/A');
insert into catalog.shims values (2, 0, 2, 'N/A');
insert into catalog.shims values (3, 1, 3, 'N/A');
insert into catalog.shims values (4, 2, 4, 'N/A');

-- catalog.scidbbinapath:
-- binary path to scidb utilities: csv2scidb, iquery, etc.
insert into catalog.scidbbinpaths values (3,'/opt/scidb/14.12/bin/');
-- insert into catalog.scidbbinpaths values (3,'/opt/scidb/14.12/bin/');
-- insert into catalog.scidbbinpaths values (4,'/opt/scidb/14.12/bin/');


-- catalog.objects
-- oid	name	fields	logical_db	physical_db
insert into catalog.objects values(0, 'mimic2v26.a_chartdurations', 'subject_id,icustay_id,itemid,elemid,starttime,startrealtime,endtime,cuid,duration', 2, 3);
insert into catalog.objects values(1, 'mimic2v26.a_iodurations', 'subject_id,icustay_id,itemid,elemid,starttime,startrealtime,endtime,cuid,duration', 2, 3);
insert into catalog.objects values(2, 'mimic2v26.a_meddurations', 'subject_id,icustay_id,itemid,elemid,starttime,startrealtime,endtime,cuid,duration', 2, 3);
insert into catalog.objects values(3, 'mimic2v26.additives', 'subject_id,icustay_id,itemid,ioitemid,charttime,elemid,cgid,cuid,amount,doseunits,route', 2, 3);
insert into catalog.objects values(4, 'mimic2v26.admissions', 'hadm_id,subject_id,admit_dt,disch_dt', 2, 2);
insert into catalog.objects values(5, 'mimic2v26.censusevents', 'census_id,subject_id,intime,outtime,careunit,destcareunit,dischstatus,los,icustay_id', 2, 2);
insert into catalog.objects values(6, 'mimic2v26.chartevents', 'subject_id,icustay_id,itemid,charttime,elemid,realtime,cgid,cuid,value1,value1num,value1uom,value2,value2num,value2uom,resultstatus,stopped', 2, 3);
insert into catalog.objects values(7, 'mimic2v26.comorbidity_scores', 'subject_id,hadm_id,category,congestive_heart_failure,cardiac_arrhythmias,valvular_disease,pulmonary_circulation,peripheral_vascular,hypertension,paralysis,other_neurological,chronic_pulmonary,diabetes_uncomplicated,diabetes_complicated,hypothyroidism,renal_failure,liver_disease,peptic_ulcer,aids,lymphoma,metastatic_cancer,solid_tumor,rheumatoid_arthritis,coagulopathy,obesity,weight_loss,fluid_electrolyte,blood_loss_anemia,deficiency_anemias,alcohol_abuse,drug_abuse,psychoses,depression', 2, 3);
insert into catalog.objects values(8, 'mimic2v26.d_caregivers', 'cgid,label', 2, 2);
insert into catalog.objects values(9, 'mimic2v26.d_careunits', 'cuid,label', 2, 2);
insert into catalog.objects values(10, 'mimic2v26.d_chartitems', 'itemid,label,category,description', 2, 2);
insert into catalog.objects values(11, 'mimic2v26.d_chartitems_detail', 'label,label_lower,itemid,category,description,value_type,value_column,rows_num,subjects_num,chart_vs_realtime_delay_mean,chart_vs_realtime_delay_stddev,value1_uom_num,value1_uom_has_nulls,value1_uom_sample1,value1_uom_sample2,value1_distinct_num,value1_has_nulls,value1_sample1,value1_sample2,value1_length_min,value1_length_max,value1_length_mean,value1num_min,value1num_max,value1num_mean,value1num_stddev,value2_uom_num,value2_uom_has_nulls,value2_uom_sample1,value2_uom_sample2,value2_distinct_num,value2_has_nulls,value2_sample1,value2_sample2,value2_length_min,value2_length_max,value2_length_mean,value2num_min,value2num_max,value2num_mean,value2num_stddev', 2, 2);
insert into catalog.objects values(12, 'mimic2v26.d_codeditems', 'itemid,code,type,category,label,description', 2, 2);
insert into catalog.objects values(13, 'mimic2v26.d_demographicitems', 'itemid,label,category', 2, 2);
insert into catalog.objects values(14, 'mimic2v26.d_ioitems', 'itemid,label,category', 2, 2);
insert into catalog.objects values(15, 'mimic2v26.d_labitems', 'itemid,test_name,fluid,category,loinc_code,loinc_description', 2, 2);
insert into catalog.objects values(16, 'mimic2v26.d_meditems', 'itemid,label', 2, 2);
insert into catalog.objects values(17, 'mimic2v26.d_parammap_items', 'category,description', 2, 2);
insert into catalog.objects values(18, 'mimic2v26.d_patients', 'subject_id,sex,dob,dod,hospital_expire_flg', 2, 3);
insert into catalog.objects values(19, 'mimic2v26.db_schema', 'created_dt,created_by,updated_dt,updated_by,schema_dt,version,comments', 2, 2);
insert into catalog.objects values(20, 'mimic2v26.deliveries', 'subject_id,icustay_id,ioitemid,charttime,elemid,cgid,cuid,site,rate,rateuom', 2, 2);
insert into catalog.objects values(21, 'mimic2v26.demographic_detail', 'subject_id,hadm_id,marital_status_itemid,marital_status_descr,ethnicity_itemid,ethnicity_descr,overall_payor_group_itemid,overall_payor_group_descr,religion_itemid,religion_descr,admission_type_itemid,admission_type_descr,admission_source_itemid,admission_source_descr', 2, 3);
insert into catalog.objects values(22, 'mimic2v26.demographicevents', 'subject_id,hadm_id,itemid', 2, 2);
insert into catalog.objects values(23, 'mimic2v26.drgevents', 'subject_id,hadm_id,itemid,cost_weight', 2, 3);
insert into catalog.objects values(24, 'mimic2v26.icd9', 'subject_id,hadm_id,sequence,code,description', 2, 3);
insert into catalog.objects values(25, 'mimic2v26.icustay_days', 'icustay_id,subject_id,seq,begintime,endtime,first_day_flg,last_day_flg', 2, 3);
insert into catalog.objects values(26, 'mimic2v26.icustay_detail', 'icustay_id,subject_id,gender,dob,dod,expire_flg,subject_icustay_total_num,subject_icustay_seq,hadm_id,hospital_total_num,hospital_seq,hospital_first_flg,hospital_last_flg,hospital_admit_dt,hospital_disch_dt,hospital_los,hospital_expire_flg,icustay_total_num,icustay_seq,icustay_first_flg,icustay_last_flg,icustay_intime,icustay_outtime,icustay_admit_age,icustay_age_group,icustay_los,icustay_expire_flg,icustay_first_careunit,icustay_last_careunit,icustay_first_service,icustay_last_service,height,weight_first,weight_min,weight_max,sapsi_first,sapsi_min,sapsi_max,sofa_first,sofa_min,sofa_max,matched_waveforms_num', 2, 3);
insert into catalog.objects values(27, 'mimic2v26.icustayevents', 'icustay_id,subject_id,intime,outtime,los,first_careunit,last_careunit', 2, 3);
insert into catalog.objects values(28, 'mimic2v26.ioevents', 'subject_id,icustay_id,itemid,charttime,elemid,altid,realtime,cgid,cuid,volume,volumeuom,unitshung,unitshunguom,newbottle,stopped,estimate', 2, 2);
insert into catalog.objects values(29, 'mimic2v26.labevents', 'subject_id,hadm_id,icustay_id,itemid,charttime,value,valuenum,flag,valueuom', 2, 2);
insert into catalog.objects values(30, 'mimic2v26.medevents', 'subject_id,icustay_id,itemid,charttime,elemid,realtime,cgid,cuid,volume,dose,doseuom,solutionid,solvolume,solunits,route,stopped', 2, 2);
insert into catalog.objects values(31, 'mimic2v26.microbiologyevents', 'subject_id,hadm_id,charttime,spec_itemid,org_itemid,isolate_num,ab_itemid,dilution_amount,dilution_comparison,interpretation', 2, 2);
insert into catalog.objects values(32, 'mimic2v26.noteevents', 'subject_id,hadm_id,icustay_id,elemid,charttime,realtime,cgid,correction,cuid,category,title,text,exam_name,patient_info', 2, 2);
insert into catalog.objects values(33, 'mimic2v26.parameter_mapping', 'param1_str,param1_num,category,param2_str,param2_num,order_num,valid_flg,comments', 2, 2);
insert into catalog.objects values(34, 'mimic2v26.poe_med', 'poe_id,drug_type,drug_name,drug_name_generic,prod_strength,form_rx,dose_val_rx,dose_unit_rx,form_val_disp,form_unit_disp,dose_val_disp,dose_unit_disp,dose_range_override', 2, 2);
insert into catalog.objects values(35, 'mimic2v26.poe_order', 'poe_id,subject_id,hadm_id,icustay_id,start_dt,stop_dt,enter_dt,medication,procedure_type,status,route,frequency,dispense_sched,iv_fluid,iv_rate,infusion_type,sliding_scale,doses_per_24hrs,duration,duration_intvl,expiration_val,expiration_unit,expiration_dt,label_instr,additional_instr,md_add_instr,rnurse_add_instr', 2, 2);
insert into catalog.objects values(36, 'mimic2v26.procedureevents', 'subject_id,hadm_id,itemid,sequence_num,proc_dt', 2, 2);
insert into catalog.objects values(37, 'mimic2v26.totalbalevents', 'subject_id,icustay_id,itemid,charttime,elemid,realtime,cgid,cuid,pervolume,cumvolume,accumperiod,approx,reset,stopped', 2, 2);

insert into catalog.objects values(38,'region','r_regionkey,r_name,r_comment',4,4);
insert into catalog.objects values(39,'nation','n_nationkey,n_name,n_regionkey,n_comment',4,5);
insert into catalog.objects values(40,'part','p_partkey,p_name,p_mfgr,p_brand,p_type,p_size,p_container,p_retailprice,p_comment',4,4);
insert into catalog.objects values(41,'supplier','s_suppkey,s_name,s_address,s_nationkey,s_phone,s_acctbal,s_comment',4,5);
insert into catalog.objects values(42,'partsupp','ps_partkey,ps_suppkey,ps_availqty,ps_supplycost,ps_comment',4,4);
insert into catalog.objects values(43,'customer','c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment',4,5);
insert into catalog.objects values(44,'orders','o_orderkey,o_custkey,o_orderstatus,o_totalprice,o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment',4,4);
insert into catalog.objects values(45,'lineitem','l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment',4,5);

insert into catalog.objects values(46,'myarray','i,j,val',2,6);
insert into catalog.objects values (47, 'note_events_Tedge', '', 7, 7);
insert into catalog.objects values (48, 'note_events_TedgeT','', 7, 7);
insert into catalog.objects values (49, 'note_events_TedgeDeg','', 7, 7);
insert into catalog.objects values (50, 'note_events_TedgeTxt', '', 7, 7);

-- insert into catalog.objects values(46,'go_matrix','geneid,goid,belongs',7,7);
-- insert into catalog.objects values(47,'genes','id,target,pos,len,func',7,7);
-- insert into catalog.objects values(48,'patients','id,age,gender,zipcode,disease,response',7,7);
-- insert into catalog.objects values(49,'geo','geneid,patientid,expr_value',7,7);
