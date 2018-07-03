CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
DROP TABLE if exists uuids;
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
  row_id SERIAL,
  diagnosis character varying(255)
);
insert into diagnoses (diagnosis) (select diagnosis from admissions group by diagnosis);
CREATE UNIQUE INDEX diagnoses_idx ON diagnoses (diagnosis);


DROP TABLE if exists locations;
CREATE TABLE mimiciii.locations
(
  row_id SERIAL,
  location character varying(50)
);
insert into locations (location)  (select admission_location from admissions group by admission_location);
insert into locations (location)  (select discharge_location from admissions group by discharge_location);
CREATE UNIQUE INDEX location_idx ON locations (location);


DROP TABLE if exists visittypes;
CREATE TABLE mimiciii.visittypes
(
  row_id SERIAL,
  visittype character varying(50)
);
insert into visittypes (visittype) (select admission_type from admissions group by admission_type);
CREATE UNIQUE INDEX visittype_idx ON visittypes (visittype);

drop table if exists deltadate;
create table deltadate as select floor(EXTRACT(epoch FROM(min(admissions.admittime)-'2000-01-01'))/(3600*24)) as offset,subject_id from admissions group by subject_id;
create table chartcounts as select label,sum(num) num,json_agg(ce.itemid) itemids from (select count(*) num,itemid from chartevents group by itemid) ce left join d_items on d_items.itemid = ce.itemid group by label order by num desc; 
