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
