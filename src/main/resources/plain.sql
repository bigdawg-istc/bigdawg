CREATE TABLE demographics (
       patient_id integer,
       birth_year integer,
       gender integer,
       race integer,
       ethnicity integer,
       insurance integer,
       zip integer);
 
CREATE TABLE diagnoses (
	patient_id integer,
	timestamp_ timestamp,
	visit_no integer,
	type_ integer,
	encounter_id integer,
	diag_src varchar(30),
	icd9 varchar(7),
	primary_ integer);

CREATE TABLE vitals (
	patient_id integer,
	height_timestamp timestamp,
	height_visit_no integer,
	height real,
	height_units varchar(10),
	weight_timestamp timestamp,
	weight_visit_no integer,
	weight real,
	weight_units varchar(10),
	bmi_timestamp timestamp,
	bmi_visit_no integer,
	bmi real,
	bmi_units varchar(10),
	pulse integer,
	systolic integer,
	diastolic integer ,
	bp_method varchar(10));       


CREATE TABLE labs (
	patient_id integer,
	timestamp_ timestamp,
	test_name varchar(30),
	value_ varchar(30),
	unit varchar(10),
	value_low real,
	value_high real);


CREATE TABLE medications (
	patient_id integer,
	timestamp_ timestamp,
	medication varchar(10),
        dosage varchar(10),
	route varchar(10));

CREATE TABLE site (
       id integer);