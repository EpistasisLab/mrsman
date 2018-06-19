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
  uuid uuid,
  diagnosis character varying(255)
);
insert into diagnoses (select uuid_generate_v4() uuid,diagnosis from admissions group by diagnosis);
CREATE UNIQUE INDEX diagnoses_idx ON diagnoses (diagnosis);


CREATE TABLE mimiciii.locations
(
  uuid uuid,
  location character varying(50)
);
insert into locations (select uuid_generate_v4() uuid,admission_location as location from admissions group by admission_location);
insert into locations (select uuid_generate_v4() uuid,discharge_location as location from admissions group by discharge_location);
CREATE UNIQUE INDEX location_idx ON locations (location);


CREATE TABLE mimiciii.visittypes
(
  uuid uuid,
  visittype character varying(50)
);
insert into visittypes (select uuid_generate_v4() uuid,admission_type as visittype from admissions group by admission_type);
CREATE UNIQUE INDEX visittype_idx ON visittypes (visittype);



insert into uuids select uuid_generate_v4(),'patients',row_id from patients;
-- insert into uuids select uuid_generate_v4(),'caregivers',row_id from caregivers;
insert into uuids select uuid_generate_v4(),'d_items',row_id from d_items;
insert into uuids select uuid_generate_v4(),'d_icd_diagnoses',row_id from d_icd_diagnoses;
--  insert into uuids select uuid_generate_v4(),'admissions',row_id from admissions;
--  insert into uuids select uuid_generate_v4(),'chartevents',row_id from chartevents;
insert into uuids select uuid_generate_v4(),'diagnoses',row_id from diagnoses;
