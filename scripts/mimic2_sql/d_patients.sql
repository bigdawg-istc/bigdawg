CREATE TABLE mimic2v26.d_patients (
    subject_id integer NOT NULL,
    sex character varying(1),
    dob timestamp without time zone NOT NULL,
    dod timestamp without time zone,
    hospital_expire_flg character varying(1) DEFAULT 'N'::character varying
);
