-- Table: mimiciii.uuid_link

-- DROP TABLE mimiciii.uuid_link;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TABLE mimiciii.uuids
(
  uuid uuid,
  src character(32),
  row_id integer
)
WITH (
  OIDS=FALSE
);
ALTER TABLE mimiciii.uuids OWNER TO postgres;
CREATE UNIQUE INDEX uuid_idx ON uuids (uuid);
CREATE UNIQUE INDEX lookup_idx ON uuids (src,row_id);

DROP TABLE if exists mimiciii.diagnoses;
CREATE TABLE mimiciii.diagnoses
(
  row_id integer,
  diagnoses character varying(255)
);
insert into diagnoses (select ROW_NUMBER() OVER (ORDER BY diagnosis) row_id,diagnosis from admissions group by diagnosis);

CREATE UNIQUE INDEX diagnoses_idx ON diagnoses (diagnoses);

insert into uuids select uuid_generate_v4(),'patients',row_id from patients;
insert into uuids select uuid_generate_v4(),'caregivers',row_id from caregivers;
insert into uuids select uuid_generate_v4(),'d_items',row_id from d_items;
insert into uuids select uuid_generate_v4(),'d_icd_diagnoses',row_id from d_icd_diagnoses;
insert into uuids select uuid_generate_v4(),'admissions',row_id from admissions;
--  insert into uuids select uuid_generate_v4(),'chartevents',row_id from chartevents;
insert into uuids select uuid_generate_v4(),'diagnoses',row_id from diagnoses;
