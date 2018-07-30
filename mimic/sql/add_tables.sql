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
insert into locations (location)  (select curr_careunit from transfers group by curr_careunit);
insert into locations (location)  (select curr_service from services group by curr_service);
delete from locations where location is null;

CREATE UNIQUE INDEX location_idx ON locations (location);


DROP TABLE if exists encountertypes;
CREATE TABLE mimiciii.encountertypes
(
  row_id SERIAL,
  encountertype character varying(50)
);
insert into encountertypes (encountertype) (select admission_type from admissions group by admission_type);
CREATE UNIQUE INDEX encountertype_idx ON encountertypes (encountertype);


DROP TABLE if exists visittypes;
CREATE TABLE mimiciii.visittypes
(
  row_id SERIAL,
  visittype character varying(50)
);
insert into visittypes (visittype) (select admission_type from admissions group by admission_type);
CREATE UNIQUE INDEX visittype_idx ON visittypes (visittype);


DROP TABLE if exists notecategories;
CREATE TABLE mimiciii.notecategories
(
  row_id SERIAL,
  category character varying(50)
);
insert into notecategories (category) (select category from noteevents group by category);
CREATE UNIQUE INDEX notecategory_idx ON notecategories (category);


-- offset to set events in the past
drop table if exists deltadate;
create table deltadate as select floor(EXTRACT(epoch FROM(min(admissions.admittime)-'2000-01-01'))/(3600*24)) as offset,subject_id from admissions group by subject_id;


--  extract text data
drop table if exists cetxt_tmp;
create temporary table cetxt_tmp as select value,itemid,count(*) num from chartevents where value ~ '[a-zA-Z]'  and valuenum is null  group by itemid,value order by itemid;

-- map common text values
drop table if exists cetxt_map;
create table cetxt_map as select cetxt_tmp.value,summary.itemid from (select itemid,round(sum(num)/count(*)) density from cetxt_tmp group by itemid order by density) summary left join cetxt_tmp on cetxt_tmp.itemid = summary.itemid where summary.density > 1000 order by itemid,value;
drop table if exists cetxt;
CREATE TABLE mimiciii.cetxt
(
  row_id SERIAL,
  value character varying(255)
);
insert into cetxt (value) select value from cetxt_map group by value order by value;


-- generate metadata for numeric chart data
create temporary table cenum_tmp_1 as select itemid,valueuom,count(*) from chartevents where valuenum is not null group by itemid,valueuom order by itemid;
create temporary table cenum_tmp_2 as select max(valuenum) max_val,min(valuenum) min_val,avg(valuenum) avg_val,itemid,count(*) from chartevents where valuenum is not null group by itemid order by itemid,count desc;

drop table if exists cenum;
create table cenum as select cenum_tmp_2.itemid,min_val,avg_val,max_val,unitcounts.valueuom units,cenum_tmp_2.count num from cenum_tmp_2 left join (SELECT (mi).* FROM (SELECT  (SELECT mi FROM cenum_tmp_1 mi WHERE  mi.itemid = m.itemid ORDER BY count DESC LIMIT 1) AS mi FROM  cenum_tmp_1 m GROUP BY itemid) q ORDER BY  (mi).itemid) unitcounts on cenum_tmp_2.itemid = unitcounts.itemid;

