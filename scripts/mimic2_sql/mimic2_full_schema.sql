--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: mimic2v26; Type: SCHEMA; Schema: -; Owner: pguser
--

CREATE SCHEMA mimic2v26;


ALTER SCHEMA mimic2v26 OWNER TO pguser;

SET search_path = mimic2v26, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: a_chartdurations; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE a_chartdurations (
    subject_id integer NOT NULL,
    icustay_id integer,
    itemid integer NOT NULL,
    elemid integer NOT NULL,
    starttime timestamp without time zone NOT NULL,
    startrealtime timestamp without time zone NOT NULL,
    endtime timestamp without time zone,
    cuid integer,
    duration double precision
);


ALTER TABLE a_chartdurations OWNER TO pguser;

--
-- Name: a_iodurations; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE a_iodurations (
    subject_id integer NOT NULL,
    icustay_id integer,
    itemid integer NOT NULL,
    elemid integer NOT NULL,
    starttime timestamp without time zone NOT NULL,
    startrealtime timestamp without time zone,
    endtime timestamp without time zone,
    cuid integer,
    duration double precision
);


ALTER TABLE a_iodurations OWNER TO pguser;

--
-- Name: a_meddurations; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE a_meddurations (
    subject_id integer NOT NULL,
    icustay_id integer,
    itemid integer NOT NULL,
    elemid integer NOT NULL,
    starttime timestamp without time zone NOT NULL,
    startrealtime timestamp without time zone,
    endtime timestamp without time zone,
    cuid integer,
    duration double precision
);


ALTER TABLE a_meddurations OWNER TO pguser;

--
-- Name: additives; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE additives (
    subject_id integer NOT NULL,
    icustay_id integer,
    itemid integer NOT NULL,
    ioitemid integer NOT NULL,
    charttime timestamp without time zone NOT NULL,
    elemid integer NOT NULL,
    cgid integer,
    cuid integer,
    amount double precision,
    doseunits character varying(20),
    route character varying(20)
);


ALTER TABLE additives OWNER TO pguser;

--
-- Name: admissions; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE admissions (
    hadm_id integer NOT NULL,
    subject_id integer NOT NULL,
    admit_dt timestamp without time zone NOT NULL,
    disch_dt timestamp without time zone NOT NULL
);


ALTER TABLE admissions OWNER TO pguser;

--
-- Name: censusevents; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE censusevents (
    census_id integer NOT NULL,
    subject_id integer NOT NULL,
    intime timestamp without time zone NOT NULL,
    outtime timestamp without time zone NOT NULL,
    careunit integer,
    destcareunit integer,
    dischstatus character varying(20),
    los double precision,
    icustay_id integer
);


ALTER TABLE censusevents OWNER TO pguser;

--
-- Name: chartevents; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE chartevents (
    subject_id integer NOT NULL,
    icustay_id integer,
    itemid integer NOT NULL,
    charttime timestamp without time zone NOT NULL,
    elemid integer NOT NULL,
    realtime timestamp without time zone NOT NULL,
    cgid integer,
    cuid integer,
    value1 character varying(110),
    value1num double precision,
    value1uom character varying(20),
    value2 character varying(110),
    value2num double precision,
    value2uom character varying(20),
    resultstatus character varying(20),
    stopped character varying(20)
);


ALTER TABLE chartevents OWNER TO pguser;

--
-- Name: comorbidity_scores; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE comorbidity_scores (
    subject_id integer NOT NULL,
    hadm_id integer NOT NULL,
    category character(10),
    congestive_heart_failure double precision,
    cardiac_arrhythmias double precision,
    valvular_disease double precision,
    pulmonary_circulation double precision,
    peripheral_vascular double precision,
    hypertension double precision,
    paralysis double precision,
    other_neurological double precision,
    chronic_pulmonary double precision,
    diabetes_uncomplicated double precision,
    diabetes_complicated double precision,
    hypothyroidism double precision,
    renal_failure double precision,
    liver_disease double precision,
    peptic_ulcer double precision,
    aids double precision,
    lymphoma double precision,
    metastatic_cancer double precision,
    solid_tumor double precision,
    rheumatoid_arthritis double precision,
    coagulopathy double precision,
    obesity double precision,
    weight_loss double precision,
    fluid_electrolyte double precision,
    blood_loss_anemia double precision,
    deficiency_anemias double precision,
    alcohol_abuse double precision,
    drug_abuse double precision,
    psychoses double precision,
    depression double precision
);


ALTER TABLE comorbidity_scores OWNER TO pguser;

--
-- Name: d_caregivers; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE d_caregivers (
    cgid integer NOT NULL,
    label character varying(6)
);


ALTER TABLE d_caregivers OWNER TO pguser;

--
-- Name: d_careunits; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE d_careunits (
    cuid integer NOT NULL,
    label character varying(20)
);


ALTER TABLE d_careunits OWNER TO pguser;

--
-- Name: d_chartitems; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE d_chartitems (
    itemid integer NOT NULL,
    label character varying(110),
    category character varying(50),
    description character varying(255)
);


ALTER TABLE d_chartitems OWNER TO pguser;

--
-- Name: d_chartitems_detail; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE d_chartitems_detail (
    label character varying(110),
    label_lower character varying(110),
    itemid integer,
    category character varying(50),
    description character varying(255),
    value_type character(1),
    value_column character varying(6),
    rows_num double precision,
    subjects_num double precision,
    chart_vs_realtime_delay_mean double precision,
    chart_vs_realtime_delay_stddev double precision,
    value1_uom_num double precision,
    value1_uom_has_nulls character(1),
    value1_uom_sample1 character varying(20),
    value1_uom_sample2 character varying(20),
    value1_distinct_num double precision,
    value1_has_nulls character(1),
    value1_sample1 character varying(110),
    value1_sample2 character varying(110),
    value1_length_min double precision,
    value1_length_max double precision,
    value1_length_mean double precision,
    value1num_min double precision,
    value1num_max double precision,
    value1num_mean double precision,
    value1num_stddev double precision,
    value2_uom_num double precision,
    value2_uom_has_nulls character(1),
    value2_uom_sample1 character varying(20),
    value2_uom_sample2 character varying(20),
    value2_distinct_num double precision,
    value2_has_nulls character(1),
    value2_sample1 character varying(110),
    value2_sample2 character varying(110),
    value2_length_min double precision,
    value2_length_max double precision,
    value2_length_mean double precision,
    value2num_min double precision,
    value2num_max double precision,
    value2num_mean double precision,
    value2num_stddev double precision
);


ALTER TABLE d_chartitems_detail OWNER TO pguser;

--
-- Name: d_codeditems; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE d_codeditems (
    itemid integer NOT NULL,
    code character varying(10),
    type character varying(12),
    category character varying(13),
    label character varying(100),
    description character varying(100)
);


ALTER TABLE d_codeditems OWNER TO pguser;

--
-- Name: d_demographicitems; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE d_demographicitems (
    itemid integer NOT NULL,
    label character varying(50),
    category character varying(19)
);


ALTER TABLE d_demographicitems OWNER TO pguser;

--
-- Name: d_ioitems; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE d_ioitems (
    itemid integer NOT NULL,
    label character varying(600),
    category character varying(50)
);


ALTER TABLE d_ioitems OWNER TO pguser;

--
-- Name: d_labitems; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE d_labitems (
    itemid integer NOT NULL,
    test_name character varying(50) NOT NULL,
    fluid character varying(50) NOT NULL,
    category character varying(50) NOT NULL,
    loinc_code character varying(7),
    loinc_description character varying(100)
);


ALTER TABLE d_labitems OWNER TO pguser;

--
-- Name: d_meditems; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE d_meditems (
    itemid integer NOT NULL,
    label character varying(20)
);


ALTER TABLE d_meditems OWNER TO pguser;

--
-- Name: d_parammap_items; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE d_parammap_items (
    category character varying(50) NOT NULL,
    description character varying(500)
);


ALTER TABLE d_parammap_items OWNER TO pguser;

--
-- Name: d_patients; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE d_patients (
    subject_id integer NOT NULL,
    sex character varying(1),
    dob timestamp without time zone NOT NULL,
    dod timestamp without time zone,
    hospital_expire_flg character varying(1) DEFAULT 'N'::character varying
);


ALTER TABLE d_patients OWNER TO pguser;

--
-- Name: db_schema; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE db_schema (
    created_dt timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone,
    created_by character varying(15) DEFAULT "current_user"(),
    updated_dt timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone,
    updated_by character varying(15) DEFAULT "current_user"(),
    schema_dt timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone,
    version character varying(25) NOT NULL,
    comments character varying(250)
);


ALTER TABLE db_schema OWNER TO pguser;

--
-- Name: deliveries; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE deliveries (
    subject_id integer NOT NULL,
    icustay_id integer,
    ioitemid integer NOT NULL,
    charttime timestamp without time zone NOT NULL,
    elemid integer NOT NULL,
    cgid integer,
    cuid integer,
    site character varying(20),
    rate double precision,
    rateuom character varying(20) NOT NULL
);


ALTER TABLE deliveries OWNER TO pguser;

--
-- Name: demographic_detail; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE demographic_detail (
    subject_id integer NOT NULL,
    hadm_id integer NOT NULL,
    marital_status_itemid integer,
    marital_status_descr character varying(50),
    ethnicity_itemid integer,
    ethnicity_descr character varying(50),
    overall_payor_group_itemid integer,
    overall_payor_group_descr character varying(50),
    religion_itemid integer,
    religion_descr character varying(50),
    admission_type_itemid integer,
    admission_type_descr character varying(50),
    admission_source_itemid integer,
    admission_source_descr character varying(50)
);


ALTER TABLE demographic_detail OWNER TO pguser;

--
-- Name: demographicevents; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE demographicevents (
    subject_id integer NOT NULL,
    hadm_id integer NOT NULL,
    itemid integer NOT NULL
);


ALTER TABLE demographicevents OWNER TO pguser;

--
-- Name: drgevents; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE drgevents (
    subject_id integer NOT NULL,
    hadm_id integer NOT NULL,
    itemid integer NOT NULL,
    cost_weight double precision
);


ALTER TABLE drgevents OWNER TO pguser;

--
-- Name: icd9; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE icd9 (
    subject_id integer NOT NULL,
    hadm_id integer NOT NULL,
    sequence integer NOT NULL,
    code character varying(100) NOT NULL,
    description character varying(255)
);


ALTER TABLE icd9 OWNER TO pguser;

--
-- Name: icustay_days; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE icustay_days (
    icustay_id integer,
    subject_id integer,
    seq integer,
    begintime timestamp without time zone,
    endtime timestamp without time zone,
    first_day_flg character(1),
    last_day_flg character(1)
);


ALTER TABLE icustay_days OWNER TO pguser;

--
-- Name: icustay_detail; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE icustay_detail (
    icustay_id integer,
    subject_id integer,
    gender character varying(1),
    dob timestamp without time zone NOT NULL,
    dod timestamp without time zone,
    expire_flg character varying(1),
    subject_icustay_total_num double precision,
    subject_icustay_seq double precision,
    hadm_id integer,
    hospital_total_num double precision,
    hospital_seq double precision,
    hospital_first_flg character(1),
    hospital_last_flg character(1),
    hospital_admit_dt timestamp without time zone,
    hospital_disch_dt timestamp without time zone,
    hospital_los double precision,
    hospital_expire_flg character(1),
    icustay_total_num double precision,
    icustay_seq double precision,
    icustay_first_flg character(1),
    icustay_last_flg character(1),
    icustay_intime timestamp without time zone NOT NULL,
    icustay_outtime timestamp without time zone NOT NULL,
    icustay_admit_age double precision,
    icustay_age_group character varying(7),
    icustay_los double precision NOT NULL,
    icustay_expire_flg character(1),
    icustay_first_careunit character varying(20),
    icustay_last_careunit character varying(20),
    icustay_first_service character varying(110),
    icustay_last_service character varying(110),
    height double precision,
    weight_first double precision,
    weight_min double precision,
    weight_max double precision,
    sapsi_first double precision,
    sapsi_min double precision,
    sapsi_max double precision,
    sofa_first double precision,
    sofa_min double precision,
    sofa_max double precision,
    matched_waveforms_num double precision
);


ALTER TABLE icustay_detail OWNER TO pguser;

--
-- Name: icustayevents; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE icustayevents (
    icustay_id integer NOT NULL,
    subject_id integer NOT NULL,
    intime timestamp without time zone NOT NULL,
    outtime timestamp without time zone NOT NULL,
    los double precision NOT NULL,
    first_careunit integer,
    last_careunit integer
);


ALTER TABLE icustayevents OWNER TO pguser;

--
-- Name: ioevents; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE ioevents (
    subject_id integer NOT NULL,
    icustay_id integer,
    itemid integer NOT NULL,
    charttime timestamp without time zone NOT NULL,
    elemid integer NOT NULL,
    altid integer,
    realtime timestamp without time zone,
    cgid integer,
    cuid integer,
    volume double precision,
    volumeuom character varying(20),
    unitshung double precision,
    unitshunguom character varying(20),
    newbottle double precision,
    stopped character varying(20),
    estimate character varying(20)
);


ALTER TABLE ioevents OWNER TO pguser;

--
-- Name: labevents; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE labevents (
    subject_id integer NOT NULL,
    hadm_id integer,
    icustay_id integer,
    itemid integer NOT NULL,
    charttime timestamp without time zone NOT NULL,
    value character varying(100),
    valuenum double precision,
    flag character varying(10),
    valueuom character varying(10)
);


ALTER TABLE labevents OWNER TO pguser;

--
-- Name: medevents; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE medevents (
    subject_id integer NOT NULL,
    icustay_id integer,
    itemid integer NOT NULL,
    charttime timestamp without time zone NOT NULL,
    elemid integer NOT NULL,
    realtime timestamp without time zone NOT NULL,
    cgid integer,
    cuid integer,
    volume double precision,
    dose double precision,
    doseuom character varying(20),
    solutionid integer,
    solvolume double precision,
    solunits character varying(20),
    route character varying(20),
    stopped character varying(20)
);


ALTER TABLE medevents OWNER TO pguser;

--
-- Name: microbiologyevents; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE microbiologyevents (
    subject_id integer,
    hadm_id integer,
    charttime timestamp without time zone,
    spec_itemid integer,
    org_itemid integer,
    isolate_num double precision,
    ab_itemid integer,
    dilution_amount character varying(72),
    dilution_comparison character varying(10),
    interpretation character varying(1)
);


ALTER TABLE microbiologyevents OWNER TO pguser;

--
-- Name: noteevents; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE noteevents (
    subject_id integer NOT NULL,
    hadm_id integer,
    icustay_id integer,
    elemid integer,
    charttime timestamp without time zone NOT NULL,
    realtime timestamp without time zone,
    cgid integer,
    correction character(1),
    cuid integer,
    category character varying(26),
    title character varying(255),
    text text,
    exam_name character varying(100),
    patient_info character varying(4000)
);


ALTER TABLE noteevents OWNER TO pguser;

--
-- Name: parameter_mapping; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE parameter_mapping (
    param1_str character varying(50),
    param1_num double precision,
    category character varying(50) NOT NULL,
    param2_str character varying(50),
    param2_num double precision,
    order_num double precision,
    valid_flg character(1) NOT NULL,
    comments character varying(255)
);


ALTER TABLE parameter_mapping OWNER TO pguser;

--
-- Name: poe_med; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE poe_med (
    poe_id bigint NOT NULL,
    drug_type character varying(20) NOT NULL,
    drug_name character varying(100) NOT NULL,
    drug_name_generic character varying(100),
    prod_strength character varying(255),
    form_rx character varying(25),
    dose_val_rx character varying(100),
    dose_unit_rx character varying(50),
    form_val_disp character varying(50),
    form_unit_disp character varying(50),
    dose_val_disp double precision,
    dose_unit_disp character varying(50),
    dose_range_override character varying(2000)
);


ALTER TABLE poe_med OWNER TO pguser;

--
-- Name: poe_order; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE poe_order (
    poe_id bigint NOT NULL,
    subject_id integer NOT NULL,
    hadm_id integer NOT NULL,
    icustay_id integer,
    start_dt timestamp without time zone,
    stop_dt timestamp without time zone,
    enter_dt timestamp without time zone NOT NULL,
    medication character varying(255),
    procedure_type character varying(50),
    status character varying(50),
    route character varying(50),
    frequency character varying(50),
    dispense_sched character varying(255),
    iv_fluid character varying(255),
    iv_rate character varying(100),
    infusion_type character varying(15),
    sliding_scale character(1),
    doses_per_24hrs double precision,
    duration double precision,
    duration_intvl character varying(15),
    expiration_val double precision,
    expiration_unit character varying(50),
    expiration_dt timestamp without time zone,
    label_instr character varying(1000),
    additional_instr character varying(1000),
    md_add_instr character varying(4000),
    rnurse_add_instr character varying(1000)
);


ALTER TABLE poe_order OWNER TO pguser;

--
-- Name: procedureevents; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE procedureevents (
    subject_id integer NOT NULL,
    hadm_id integer NOT NULL,
    itemid integer,
    sequence_num integer NOT NULL,
    proc_dt timestamp without time zone
);


ALTER TABLE procedureevents OWNER TO pguser;

--
-- Name: totalbalevents; Type: TABLE; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE TABLE totalbalevents (
    subject_id integer NOT NULL,
    icustay_id integer,
    itemid integer NOT NULL,
    charttime timestamp without time zone NOT NULL,
    elemid integer NOT NULL,
    realtime timestamp without time zone NOT NULL,
    cgid integer,
    cuid integer,
    pervolume double precision,
    cumvolume double precision,
    accumperiod character varying(20),
    approx character varying(20),
    reset double precision,
    stopped character varying(20)
);


ALTER TABLE totalbalevents OWNER TO pguser;

--
-- Name: admissions_pkey; Type: CONSTRAINT; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

ALTER TABLE ONLY admissions
    ADD CONSTRAINT admissions_pkey PRIMARY KEY (hadm_id);


--
-- Name: censusevents_pkey; Type: CONSTRAINT; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

ALTER TABLE ONLY censusevents
    ADD CONSTRAINT censusevents_pkey PRIMARY KEY (census_id);


--
-- Name: d_caregivers_pkey; Type: CONSTRAINT; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

ALTER TABLE ONLY d_caregivers
    ADD CONSTRAINT d_caregivers_pkey PRIMARY KEY (cgid);


--
-- Name: d_careunits_pkey; Type: CONSTRAINT; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

ALTER TABLE ONLY d_careunits
    ADD CONSTRAINT d_careunits_pkey PRIMARY KEY (cuid);


--
-- Name: d_chartitems_pkey; Type: CONSTRAINT; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

ALTER TABLE ONLY d_chartitems
    ADD CONSTRAINT d_chartitems_pkey PRIMARY KEY (itemid);


--
-- Name: d_codeditems_pkey; Type: CONSTRAINT; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

ALTER TABLE ONLY d_codeditems
    ADD CONSTRAINT d_codeditems_pkey PRIMARY KEY (itemid);


--
-- Name: d_demographicitems_pkey; Type: CONSTRAINT; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

ALTER TABLE ONLY d_demographicitems
    ADD CONSTRAINT d_demographicitems_pkey PRIMARY KEY (itemid);


--
-- Name: d_ioitems_pkey; Type: CONSTRAINT; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

ALTER TABLE ONLY d_ioitems
    ADD CONSTRAINT d_ioitems_pkey PRIMARY KEY (itemid);


--
-- Name: d_labitems_pkey; Type: CONSTRAINT; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

ALTER TABLE ONLY d_labitems
    ADD CONSTRAINT d_labitems_pkey PRIMARY KEY (itemid);


--
-- Name: d_meditems_pkey; Type: CONSTRAINT; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

ALTER TABLE ONLY d_meditems
    ADD CONSTRAINT d_meditems_pkey PRIMARY KEY (itemid);


--
-- Name: d_patients_pkey; Type: CONSTRAINT; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

ALTER TABLE ONLY d_patients
    ADD CONSTRAINT d_patients_pkey PRIMARY KEY (subject_id);


--
-- Name: demographicevents_pkey; Type: CONSTRAINT; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

ALTER TABLE ONLY demographicevents
    ADD CONSTRAINT demographicevents_pkey PRIMARY KEY (subject_id, hadm_id, itemid);


--
-- Name: drgevents_pkey; Type: CONSTRAINT; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

ALTER TABLE ONLY drgevents
    ADD CONSTRAINT drgevents_pkey PRIMARY KEY (subject_id, hadm_id, itemid);


--
-- Name: icustayevents_pkey; Type: CONSTRAINT; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

ALTER TABLE ONLY icustayevents
    ADD CONSTRAINT icustayevents_pkey PRIMARY KEY (icustay_id);


--
-- Name: poe_order_pkey; Type: CONSTRAINT; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

ALTER TABLE ONLY poe_order
    ADD CONSTRAINT poe_order_pkey PRIMARY KEY (poe_id);


--
-- Name: procedureevents_pkey; Type: CONSTRAINT; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

ALTER TABLE ONLY procedureevents
    ADD CONSTRAINT procedureevents_pkey PRIMARY KEY (subject_id, hadm_id, sequence_num);


--
-- Name: a_chartdurations_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX a_chartdurations_o1 ON a_chartdurations USING btree (cuid);


--
-- Name: a_chartdurations_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX a_chartdurations_o2 ON a_chartdurations USING btree (subject_id);


--
-- Name: a_chartdurations_o3; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX a_chartdurations_o3 ON a_chartdurations USING btree (icustay_id);


--
-- Name: a_chartdurations_o4; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX a_chartdurations_o4 ON a_chartdurations USING btree (itemid);


--
-- Name: a_iodurations_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX a_iodurations_o1 ON a_iodurations USING btree (cuid);


--
-- Name: a_iodurations_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX a_iodurations_o2 ON a_iodurations USING btree (subject_id);


--
-- Name: a_iodurations_o3; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX a_iodurations_o3 ON a_iodurations USING btree (itemid);


--
-- Name: a_iodurations_pk; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE UNIQUE INDEX a_iodurations_pk ON a_iodurations USING btree (subject_id, itemid, elemid, starttime);


--
-- Name: a_meddurations_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX a_meddurations_o1 ON a_meddurations USING btree (cuid);


--
-- Name: a_meddurations_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX a_meddurations_o2 ON a_meddurations USING btree (subject_id);


--
-- Name: a_meddurations_o3; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX a_meddurations_o3 ON a_meddurations USING btree (itemid);


--
-- Name: a_meddurations_o4; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX a_meddurations_o4 ON a_meddurations USING btree (icustay_id);


--
-- Name: a_meddurations_pk; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE UNIQUE INDEX a_meddurations_pk ON a_meddurations USING btree (subject_id, itemid, elemid, starttime);


--
-- Name: additives_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX additives_o1 ON additives USING btree (cuid);


--
-- Name: additives_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX additives_o2 ON additives USING btree (subject_id);


--
-- Name: additives_o3; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX additives_o3 ON additives USING btree (ioitemid);


--
-- Name: additives_o4; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX additives_o4 ON additives USING btree (itemid);


--
-- Name: additives_o5; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX additives_o5 ON additives USING btree (icustay_id);


--
-- Name: additives_o6; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX additives_o6 ON additives USING btree (cgid);


--
-- Name: admissions_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX admissions_o1 ON admissions USING btree (subject_id);


--
-- Name: censusevents_01; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX censusevents_01 ON censusevents USING btree (destcareunit);


--
-- Name: censusevents_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX censusevents_o2 ON censusevents USING btree (subject_id);


--
-- Name: censusevents_o3; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX censusevents_o3 ON censusevents USING btree (icustay_id);


--
-- Name: censusevents_o4; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX censusevents_o4 ON censusevents USING btree (careunit);


--
-- Name: censusevents_u1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX censusevents_u1 ON censusevents USING btree (subject_id, intime);


--
-- Name: chartevents_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX chartevents_o1 ON chartevents USING btree (cuid);


--
-- Name: chartevents_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX chartevents_o2 ON chartevents USING btree (subject_id, itemid);


--
-- Name: chartevents_o3; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX chartevents_o3 ON chartevents USING btree (subject_id);


--
-- Name: chartevents_o4; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX chartevents_o4 ON chartevents USING btree (itemid);


--
-- Name: chartevents_o5; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX chartevents_o5 ON chartevents USING btree (icustay_id);


--
-- Name: chartevents_o6; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX chartevents_o6 ON chartevents USING btree (charttime);


--
-- Name: chartevents_o7; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX chartevents_o7 ON chartevents USING btree (cgid);


--
-- Name: comorbidity_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX comorbidity_o1 ON comorbidity_scores USING btree (subject_id);


--
-- Name: comorbidity_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX comorbidity_o2 ON comorbidity_scores USING btree (hadm_id);


--
-- Name: comorbidity_o3; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX comorbidity_o3 ON comorbidity_scores USING btree (subject_id, hadm_id);


--
-- Name: d_chartitems_detail_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX d_chartitems_detail_o1 ON d_chartitems_detail USING btree (label_lower);


--
-- Name: d_chartitems_detail_u1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE UNIQUE INDEX d_chartitems_detail_u1 ON d_chartitems_detail USING btree (itemid);


--
-- Name: d_chartitems_detail_u2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE UNIQUE INDEX d_chartitems_detail_u2 ON d_chartitems_detail USING btree (itemid, label, label_lower, category);


--
-- Name: deliveries_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX deliveries_o1 ON deliveries USING btree (cuid);


--
-- Name: deliveries_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX deliveries_o2 ON deliveries USING btree (subject_id);


--
-- Name: deliveries_o3; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX deliveries_o3 ON deliveries USING btree (ioitemid);


--
-- Name: deliveries_o4; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX deliveries_o4 ON deliveries USING btree (icustay_id);


--
-- Name: deliveries_o5; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX deliveries_o5 ON deliveries USING btree (cgid);


--
-- Name: deliveries_pk; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE UNIQUE INDEX deliveries_pk ON deliveries USING btree (subject_id, ioitemid, charttime, elemid);


--
-- Name: demographicevents_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX demographicevents_o1 ON demographicevents USING btree (hadm_id);


--
-- Name: demographicevents_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX demographicevents_o2 ON demographicevents USING btree (itemid);


--
-- Name: drgevents_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX drgevents_o1 ON drgevents USING btree (hadm_id);


--
-- Name: drgevents_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX drgevents_o2 ON drgevents USING btree (itemid);


--
-- Name: drgevents_o32; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX drgevents_o32 ON drgevents USING btree (subject_id);


--
-- Name: icd9_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX icd9_o2 ON icd9 USING btree (hadm_id);


--
-- Name: icu9_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX icu9_o1 ON icd9 USING btree (subject_id);


--
-- Name: icustay_days_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX icustay_days_o1 ON icustay_days USING btree (icustay_id, seq);


--
-- Name: icustay_detail_pk; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE UNIQUE INDEX icustay_detail_pk ON icustay_detail USING btree (icustay_id);


--
-- Name: icustay_detail_u1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE UNIQUE INDEX icustay_detail_u1 ON icustay_detail USING btree (subject_id, icustay_id);


--
-- Name: icustayev_u1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE UNIQUE INDEX icustayev_u1 ON icustayevents USING btree (subject_id, intime);


--
-- Name: icustayevents_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX icustayevents_o1 ON icustayevents USING btree (intime);


--
-- Name: icustayevents_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX icustayevents_o2 ON icustayevents USING btree (outtime);


--
-- Name: icustayevents_o3; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX icustayevents_o3 ON icustayevents USING btree (last_careunit);


--
-- Name: icustayevents_o4; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX icustayevents_o4 ON icustayevents USING btree (first_careunit);


--
-- Name: ioevents_o6; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX ioevents_o6 ON ioevents USING btree (cgid);


--
-- Name: labevents_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX labevents_o1 ON labevents USING btree (subject_id);


--
-- Name: labevents_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX labevents_o2 ON labevents USING btree (hadm_id);


--
-- Name: labevents_o3; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX labevents_o3 ON labevents USING btree (icustay_id);


--
-- Name: labevents_o4; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX labevents_o4 ON labevents USING btree (itemid);


--
-- Name: labevents_o5; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX labevents_o5 ON labevents USING btree (icustay_id, itemid);


--
-- Name: medevents_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX medevents_o1 ON medevents USING btree (cuid);


--
-- Name: medevents_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX medevents_o2 ON medevents USING btree (subject_id);


--
-- Name: medevents_o3; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX medevents_o3 ON medevents USING btree (itemid);


--
-- Name: medevents_o4; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX medevents_o4 ON medevents USING btree (solutionid);


--
-- Name: medevents_o5; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX medevents_o5 ON medevents USING btree (icustay_id);


--
-- Name: medevents_o6; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX medevents_o6 ON medevents USING btree (cgid);


--
-- Name: medevents_pk; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE UNIQUE INDEX medevents_pk ON medevents USING btree (subject_id, itemid, charttime, elemid);


--
-- Name: microbiologyevents_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX microbiologyevents_o1 ON microbiologyevents USING btree (ab_itemid);


--
-- Name: microbiologyevents_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX microbiologyevents_o2 ON microbiologyevents USING btree (hadm_id);


--
-- Name: microbiologyevents_o3; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX microbiologyevents_o3 ON microbiologyevents USING btree (subject_id);


--
-- Name: microbiologyevents_o4; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX microbiologyevents_o4 ON microbiologyevents USING btree (org_itemid);


--
-- Name: microbiologyevents_o5; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX microbiologyevents_o5 ON microbiologyevents USING btree (spec_itemid);


--
-- Name: noteevents_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX noteevents_o1 ON noteevents USING btree (cuid);


--
-- Name: noteevents_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX noteevents_o2 ON noteevents USING btree (subject_id);


--
-- Name: noteevents_o3; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX noteevents_o3 ON noteevents USING btree (icustay_id);


--
-- Name: noteevents_o4; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX noteevents_o4 ON noteevents USING btree (hadm_id);


--
-- Name: noteevents_o5; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX noteevents_o5 ON noteevents USING btree (category);


--
-- Name: noteevents_o6; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX noteevents_o6 ON noteevents USING btree (cgid);


--
-- Name: poe_med_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX poe_med_o1 ON poe_med USING btree (poe_id);


--
-- Name: poe_order_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX poe_order_o1 ON poe_order USING btree (subject_id, hadm_id);


--
-- Name: poe_order_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX poe_order_o2 ON poe_order USING btree (subject_id);


--
-- Name: poe_order_o3; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX poe_order_o3 ON poe_order USING btree (hadm_id);


--
-- Name: poe_order_o4; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX poe_order_o4 ON poe_order USING btree (icustay_id);


--
-- Name: procedureevents_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX procedureevents_o1 ON procedureevents USING btree (hadm_id);


--
-- Name: procedureevents_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX procedureevents_o2 ON procedureevents USING btree (itemid);


--
-- Name: totalbalevents_o1; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX totalbalevents_o1 ON totalbalevents USING btree (cuid);


--
-- Name: totalbalevents_o2; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX totalbalevents_o2 ON totalbalevents USING btree (subject_id);


--
-- Name: totalbalevents_o3; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX totalbalevents_o3 ON totalbalevents USING btree (itemid);


--
-- Name: totalbalevents_o4; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX totalbalevents_o4 ON totalbalevents USING btree (icustay_id);


--
-- Name: totalbalevents_o5; Type: INDEX; Schema: mimic2v26; Owner: pguser; Tablespace: 
--

CREATE INDEX totalbalevents_o5 ON totalbalevents USING btree (cgid);


--
-- Name: a_chartd_fk_d_careun; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY a_chartdurations
    ADD CONSTRAINT a_chartd_fk_d_careun FOREIGN KEY (cuid) REFERENCES d_careunits(cuid) DEFERRABLE;


--
-- Name: a_chartd_fk_d_charti; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY a_chartdurations
    ADD CONSTRAINT a_chartd_fk_d_charti FOREIGN KEY (itemid) REFERENCES d_chartitems(itemid) DEFERRABLE;


--
-- Name: a_chartd_fk_d_patien; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY a_chartdurations
    ADD CONSTRAINT a_chartd_fk_d_patien FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: a_chartd_fk_icustay; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY a_chartdurations
    ADD CONSTRAINT a_chartd_fk_icustay FOREIGN KEY (icustay_id) REFERENCES icustayevents(icustay_id) DEFERRABLE;


--
-- Name: a_iodura_fk_d_careun; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY a_iodurations
    ADD CONSTRAINT a_iodura_fk_d_careun FOREIGN KEY (cuid) REFERENCES d_careunits(cuid) DEFERRABLE;


--
-- Name: a_iodura_fk_d_patien; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY a_iodurations
    ADD CONSTRAINT a_iodura_fk_d_patien FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: a_iodura_fk_icustay; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY a_iodurations
    ADD CONSTRAINT a_iodura_fk_icustay FOREIGN KEY (icustay_id) REFERENCES icustayevents(icustay_id) DEFERRABLE;


--
-- Name: a_meddur_fk_d_careun; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY a_meddurations
    ADD CONSTRAINT a_meddur_fk_d_careun FOREIGN KEY (cuid) REFERENCES d_careunits(cuid) DEFERRABLE;


--
-- Name: a_meddur_fk_d_patien; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY a_meddurations
    ADD CONSTRAINT a_meddur_fk_d_patien FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: a_meddur_fk_icustay; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY a_meddurations
    ADD CONSTRAINT a_meddur_fk_icustay FOREIGN KEY (icustay_id) REFERENCES icustayevents(icustay_id) DEFERRABLE;


--
-- Name: additives_fk_d_careun; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY additives
    ADD CONSTRAINT additives_fk_d_careun FOREIGN KEY (cuid) REFERENCES d_careunits(cuid) DEFERRABLE;


--
-- Name: additives_fk_d_patien; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY additives
    ADD CONSTRAINT additives_fk_d_patien FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: additives_fk_icustay; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY additives
    ADD CONSTRAINT additives_fk_icustay FOREIGN KEY (icustay_id) REFERENCES icustayevents(icustay_id) DEFERRABLE;


--
-- Name: admissions_fk_d_pati; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY admissions
    ADD CONSTRAINT admissions_fk_d_pati FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: censusev_fk2_d_careun; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY censusevents
    ADD CONSTRAINT censusev_fk2_d_careun FOREIGN KEY (destcareunit) REFERENCES d_careunits(cuid) DEFERRABLE;


--
-- Name: censusev_fk_d_careun; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY censusevents
    ADD CONSTRAINT censusev_fk_d_careun FOREIGN KEY (careunit) REFERENCES d_careunits(cuid) DEFERRABLE;


--
-- Name: censusev_fk_d_patien; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY censusevents
    ADD CONSTRAINT censusev_fk_d_patien FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: censusev_fk_icustay; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY censusevents
    ADD CONSTRAINT censusev_fk_icustay FOREIGN KEY (icustay_id) REFERENCES icustayevents(icustay_id) DEFERRABLE;


--
-- Name: charteve_fk_d_careun; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY chartevents
    ADD CONSTRAINT charteve_fk_d_careun FOREIGN KEY (cuid) REFERENCES d_careunits(cuid) DEFERRABLE;


--
-- Name: charteve_fk_d_charti; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY chartevents
    ADD CONSTRAINT charteve_fk_d_charti FOREIGN KEY (itemid) REFERENCES d_chartitems(itemid) DEFERRABLE;


--
-- Name: charteve_fk_d_patien; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY chartevents
    ADD CONSTRAINT charteve_fk_d_patien FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: charteve_fk_icustay; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY chartevents
    ADD CONSTRAINT charteve_fk_icustay FOREIGN KEY (icustay_id) REFERENCES icustayevents(icustay_id) DEFERRABLE;


--
-- Name: deliveri_fk_d_careun; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY deliveries
    ADD CONSTRAINT deliveri_fk_d_careun FOREIGN KEY (cuid) REFERENCES d_careunits(cuid) DEFERRABLE;


--
-- Name: deliveri_fk_d_patien; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY deliveries
    ADD CONSTRAINT deliveri_fk_d_patien FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: deliveri_fk_icustay; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY deliveries
    ADD CONSTRAINT deliveri_fk_icustay FOREIGN KEY (icustay_id) REFERENCES icustayevents(icustay_id) DEFERRABLE;


--
-- Name: demographicevents_fk_admit; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY demographicevents
    ADD CONSTRAINT demographicevents_fk_admit FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id) DEFERRABLE;


--
-- Name: demographicevents_fk_d_patien; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY demographicevents
    ADD CONSTRAINT demographicevents_fk_d_patien FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: demographicevents_fk_itemid; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY demographicevents
    ADD CONSTRAINT demographicevents_fk_itemid FOREIGN KEY (itemid) REFERENCES d_demographicitems(itemid) DEFERRABLE;


--
-- Name: drgevents_fk_admit; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY drgevents
    ADD CONSTRAINT drgevents_fk_admit FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id) DEFERRABLE;


--
-- Name: drgevents_fk_d_patien; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY drgevents
    ADD CONSTRAINT drgevents_fk_d_patien FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: drgevents_fk_itemid; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY drgevents
    ADD CONSTRAINT drgevents_fk_itemid FOREIGN KEY (itemid) REFERENCES d_codeditems(itemid) DEFERRABLE;


--
-- Name: icd9_fk_admiss; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY icd9
    ADD CONSTRAINT icd9_fk_admiss FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id) DEFERRABLE;


--
-- Name: icd9_fk_d_patien; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY icd9
    ADD CONSTRAINT icd9_fk_d_patien FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: icustayev_fk2_d_careu; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY icustayevents
    ADD CONSTRAINT icustayev_fk2_d_careu FOREIGN KEY (last_careunit) REFERENCES d_careunits(cuid) DEFERRABLE;


--
-- Name: icustayev_fk_d_careu; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY icustayevents
    ADD CONSTRAINT icustayev_fk_d_careu FOREIGN KEY (first_careunit) REFERENCES d_careunits(cuid) DEFERRABLE;


--
-- Name: icustayev_fk_d_pat; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY icustayevents
    ADD CONSTRAINT icustayev_fk_d_pat FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: ioevents_fk_d_careun; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY ioevents
    ADD CONSTRAINT ioevents_fk_d_careun FOREIGN KEY (cuid) REFERENCES d_careunits(cuid) DEFERRABLE;


--
-- Name: ioevents_fk_d_patien; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY ioevents
    ADD CONSTRAINT ioevents_fk_d_patien FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: ioevents_fk_icustay; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY ioevents
    ADD CONSTRAINT ioevents_fk_icustay FOREIGN KEY (icustay_id) REFERENCES icustayevents(icustay_id) DEFERRABLE;


--
-- Name: labevents_fk_hadm_id; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY labevents
    ADD CONSTRAINT labevents_fk_hadm_id FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id) DEFERRABLE;


--
-- Name: labevents_fk_icustay_id; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY labevents
    ADD CONSTRAINT labevents_fk_icustay_id FOREIGN KEY (icustay_id) REFERENCES icustayevents(icustay_id) DEFERRABLE;


--
-- Name: labevents_fk_subject_id; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY labevents
    ADD CONSTRAINT labevents_fk_subject_id FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: medevent_fk_d_careun; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY medevents
    ADD CONSTRAINT medevent_fk_d_careun FOREIGN KEY (cuid) REFERENCES d_careunits(cuid) DEFERRABLE;


--
-- Name: medevent_fk_d_patien; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY medevents
    ADD CONSTRAINT medevent_fk_d_patien FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: medevents_fk_icustay; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY medevents
    ADD CONSTRAINT medevents_fk_icustay FOREIGN KEY (icustay_id) REFERENCES icustayevents(icustay_id) DEFERRABLE;


--
-- Name: microbioev_ab_fk_d_coded; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY microbiologyevents
    ADD CONSTRAINT microbioev_ab_fk_d_coded FOREIGN KEY (ab_itemid) REFERENCES d_codeditems(itemid) DEFERRABLE;


--
-- Name: microbioev_fk_admissions; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY microbiologyevents
    ADD CONSTRAINT microbioev_fk_admissions FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id) DEFERRABLE;


--
-- Name: microbioev_fk_d_patients; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY microbiologyevents
    ADD CONSTRAINT microbioev_fk_d_patients FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: microbioev_org_fk_d_coded; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY microbiologyevents
    ADD CONSTRAINT microbioev_org_fk_d_coded FOREIGN KEY (org_itemid) REFERENCES d_codeditems(itemid) DEFERRABLE;


--
-- Name: microbioev_spec_fk_d_coded; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY microbiologyevents
    ADD CONSTRAINT microbioev_spec_fk_d_coded FOREIGN KEY (spec_itemid) REFERENCES d_codeditems(itemid) DEFERRABLE;


--
-- Name: noteeven_fk_adm; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY noteevents
    ADD CONSTRAINT noteeven_fk_adm FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id) DEFERRABLE;


--
-- Name: noteeven_fk_d_careun; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY noteevents
    ADD CONSTRAINT noteeven_fk_d_careun FOREIGN KEY (cuid) REFERENCES d_careunits(cuid) DEFERRABLE;


--
-- Name: noteeven_fk_d_patien; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY noteevents
    ADD CONSTRAINT noteeven_fk_d_patien FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: noteeven_fk_icustay; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY noteevents
    ADD CONSTRAINT noteeven_fk_icustay FOREIGN KEY (icustay_id) REFERENCES icustayevents(icustay_id) DEFERRABLE;


--
-- Name: poe_med_poe_order_fk1; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY poe_med
    ADD CONSTRAINT poe_med_poe_order_fk1 FOREIGN KEY (poe_id) REFERENCES poe_order(poe_id) DEFERRABLE;


--
-- Name: poe_order_fk_admiss; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY poe_order
    ADD CONSTRAINT poe_order_fk_admiss FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id) DEFERRABLE;


--
-- Name: poe_order_fk_d_patien; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY poe_order
    ADD CONSTRAINT poe_order_fk_d_patien FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: poe_order_fk_icustay; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY poe_order
    ADD CONSTRAINT poe_order_fk_icustay FOREIGN KEY (icustay_id) REFERENCES icustayevents(icustay_id) DEFERRABLE;


--
-- Name: procedureevents_fk_adm; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY procedureevents
    ADD CONSTRAINT procedureevents_fk_adm FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id) DEFERRABLE;


--
-- Name: procedureevents_fk_d_coded; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY procedureevents
    ADD CONSTRAINT procedureevents_fk_d_coded FOREIGN KEY (itemid) REFERENCES d_codeditems(itemid) DEFERRABLE;


--
-- Name: procedureevents_fk_d_patien; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY procedureevents
    ADD CONSTRAINT procedureevents_fk_d_patien FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: totalbal_fk_d_careun; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY totalbalevents
    ADD CONSTRAINT totalbal_fk_d_careun FOREIGN KEY (cuid) REFERENCES d_careunits(cuid) DEFERRABLE;


--
-- Name: totalbal_fk_d_patien; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY totalbalevents
    ADD CONSTRAINT totalbal_fk_d_patien FOREIGN KEY (subject_id) REFERENCES d_patients(subject_id) DEFERRABLE;


--
-- Name: totalbal_fk_icustay; Type: FK CONSTRAINT; Schema: mimic2v26; Owner: pguser
--

ALTER TABLE ONLY totalbalevents
    ADD CONSTRAINT totalbal_fk_icustay FOREIGN KEY (icustay_id) REFERENCES icustayevents(icustay_id) DEFERRABLE;


--
-- Name: a_chartdurations; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE a_chartdurations FROM PUBLIC;
REVOKE ALL ON TABLE a_chartdurations FROM pguser;
GRANT ALL ON TABLE a_chartdurations TO pguser;


--
-- Name: a_iodurations; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE a_iodurations FROM PUBLIC;
REVOKE ALL ON TABLE a_iodurations FROM pguser;
GRANT ALL ON TABLE a_iodurations TO pguser;


--
-- Name: a_meddurations; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE a_meddurations FROM PUBLIC;
REVOKE ALL ON TABLE a_meddurations FROM pguser;
GRANT ALL ON TABLE a_meddurations TO pguser;


--
-- Name: additives; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE additives FROM PUBLIC;
REVOKE ALL ON TABLE additives FROM pguser;
GRANT ALL ON TABLE additives TO pguser;


--
-- Name: admissions; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE admissions FROM PUBLIC;
REVOKE ALL ON TABLE admissions FROM pguser;
GRANT ALL ON TABLE admissions TO pguser;


--
-- Name: censusevents; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE censusevents FROM PUBLIC;
REVOKE ALL ON TABLE censusevents FROM pguser;
GRANT ALL ON TABLE censusevents TO pguser;


--
-- Name: chartevents; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE chartevents FROM PUBLIC;
REVOKE ALL ON TABLE chartevents FROM pguser;
GRANT ALL ON TABLE chartevents TO pguser;


--
-- Name: comorbidity_scores; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE comorbidity_scores FROM PUBLIC;
REVOKE ALL ON TABLE comorbidity_scores FROM pguser;
GRANT ALL ON TABLE comorbidity_scores TO pguser;


--
-- Name: d_caregivers; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE d_caregivers FROM PUBLIC;
REVOKE ALL ON TABLE d_caregivers FROM pguser;
GRANT ALL ON TABLE d_caregivers TO pguser;


--
-- Name: d_careunits; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE d_careunits FROM PUBLIC;
REVOKE ALL ON TABLE d_careunits FROM pguser;
GRANT ALL ON TABLE d_careunits TO pguser;


--
-- Name: d_chartitems; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE d_chartitems FROM PUBLIC;
REVOKE ALL ON TABLE d_chartitems FROM pguser;
GRANT ALL ON TABLE d_chartitems TO pguser;


--
-- Name: d_chartitems_detail; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE d_chartitems_detail FROM PUBLIC;
REVOKE ALL ON TABLE d_chartitems_detail FROM pguser;
GRANT ALL ON TABLE d_chartitems_detail TO pguser;


--
-- Name: d_codeditems; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE d_codeditems FROM PUBLIC;
REVOKE ALL ON TABLE d_codeditems FROM pguser;
GRANT ALL ON TABLE d_codeditems TO pguser;


--
-- Name: d_demographicitems; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE d_demographicitems FROM PUBLIC;
REVOKE ALL ON TABLE d_demographicitems FROM pguser;
GRANT ALL ON TABLE d_demographicitems TO pguser;


--
-- Name: d_ioitems; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE d_ioitems FROM PUBLIC;
REVOKE ALL ON TABLE d_ioitems FROM pguser;
GRANT ALL ON TABLE d_ioitems TO pguser;


--
-- Name: d_labitems; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE d_labitems FROM PUBLIC;
REVOKE ALL ON TABLE d_labitems FROM pguser;
GRANT ALL ON TABLE d_labitems TO pguser;


--
-- Name: d_meditems; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE d_meditems FROM PUBLIC;
REVOKE ALL ON TABLE d_meditems FROM pguser;
GRANT ALL ON TABLE d_meditems TO pguser;


--
-- Name: d_parammap_items; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE d_parammap_items FROM PUBLIC;
REVOKE ALL ON TABLE d_parammap_items FROM pguser;
GRANT ALL ON TABLE d_parammap_items TO pguser;


--
-- Name: d_patients; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE d_patients FROM PUBLIC;
REVOKE ALL ON TABLE d_patients FROM pguser;
GRANT ALL ON TABLE d_patients TO pguser;


--
-- Name: db_schema; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE db_schema FROM PUBLIC;
REVOKE ALL ON TABLE db_schema FROM pguser;
GRANT ALL ON TABLE db_schema TO pguser;


--
-- Name: deliveries; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE deliveries FROM PUBLIC;
REVOKE ALL ON TABLE deliveries FROM pguser;
GRANT ALL ON TABLE deliveries TO pguser;


--
-- Name: demographic_detail; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE demographic_detail FROM PUBLIC;
REVOKE ALL ON TABLE demographic_detail FROM pguser;
GRANT ALL ON TABLE demographic_detail TO pguser;


--
-- Name: demographicevents; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE demographicevents FROM PUBLIC;
REVOKE ALL ON TABLE demographicevents FROM pguser;
GRANT ALL ON TABLE demographicevents TO pguser;


--
-- Name: drgevents; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE drgevents FROM PUBLIC;
REVOKE ALL ON TABLE drgevents FROM pguser;
GRANT ALL ON TABLE drgevents TO pguser;


--
-- Name: icd9; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE icd9 FROM PUBLIC;
REVOKE ALL ON TABLE icd9 FROM pguser;
GRANT ALL ON TABLE icd9 TO pguser;


--
-- Name: icustay_days; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE icustay_days FROM PUBLIC;
REVOKE ALL ON TABLE icustay_days FROM pguser;
GRANT ALL ON TABLE icustay_days TO pguser;


--
-- Name: icustay_detail; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE icustay_detail FROM PUBLIC;
REVOKE ALL ON TABLE icustay_detail FROM pguser;
GRANT ALL ON TABLE icustay_detail TO pguser;


--
-- Name: icustayevents; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE icustayevents FROM PUBLIC;
REVOKE ALL ON TABLE icustayevents FROM pguser;
GRANT ALL ON TABLE icustayevents TO pguser;


--
-- Name: ioevents; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE ioevents FROM PUBLIC;
REVOKE ALL ON TABLE ioevents FROM pguser;
GRANT ALL ON TABLE ioevents TO pguser;


--
-- Name: labevents; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE labevents FROM PUBLIC;
REVOKE ALL ON TABLE labevents FROM pguser;
GRANT ALL ON TABLE labevents TO pguser;


--
-- Name: medevents; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE medevents FROM PUBLIC;
REVOKE ALL ON TABLE medevents FROM pguser;
GRANT ALL ON TABLE medevents TO pguser;


--
-- Name: microbiologyevents; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE microbiologyevents FROM PUBLIC;
REVOKE ALL ON TABLE microbiologyevents FROM pguser;
GRANT ALL ON TABLE microbiologyevents TO pguser;


--
-- Name: noteevents; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE noteevents FROM PUBLIC;
REVOKE ALL ON TABLE noteevents FROM pguser;
GRANT ALL ON TABLE noteevents TO pguser;


--
-- Name: parameter_mapping; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE parameter_mapping FROM PUBLIC;
REVOKE ALL ON TABLE parameter_mapping FROM pguser;
GRANT ALL ON TABLE parameter_mapping TO pguser;


--
-- Name: poe_med; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE poe_med FROM PUBLIC;
REVOKE ALL ON TABLE poe_med FROM pguser;
GRANT ALL ON TABLE poe_med TO pguser;


--
-- Name: poe_order; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE poe_order FROM PUBLIC;
REVOKE ALL ON TABLE poe_order FROM pguser;
GRANT ALL ON TABLE poe_order TO pguser;


--
-- Name: procedureevents; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE procedureevents FROM PUBLIC;
REVOKE ALL ON TABLE procedureevents FROM pguser;
GRANT ALL ON TABLE procedureevents TO pguser;


--
-- Name: totalbalevents; Type: ACL; Schema: mimic2v26; Owner: pguser
--

REVOKE ALL ON TABLE totalbalevents FROM PUBLIC;
REVOKE ALL ON TABLE totalbalevents FROM pguser;
GRANT ALL ON TABLE totalbalevents TO pguser;


--
-- PostgreSQL database dump complete
--

