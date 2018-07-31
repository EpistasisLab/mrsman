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


-- offset to set events in the past
drop table if exists deltadate;
create table deltadate as select floor(EXTRACT(epoch FROM(min(admissions.admittime)-'2000-01-01'))/(3600*24)) as offset,subject_id from admissions group by subject_id;

-- Process text chart data
--  extract distinct values and items where value is alphabetical 
drop table if exists cetxt_tmp;
create temporary table cetxt_tmp as select value,itemid,count(*) num from chartevents where value ~ '[a-zA-Z]'  and valuenum is null  group by itemid,value order by itemid;

-- Generate concepts
drop table if exists concepts;
CREATE TABLE mimiciii.concepts
(
  row_id SERIAL,
  itemid integer,
  openmrs_id integer,
  shortname character varying(255),
  longname character varying(255),
  linksto character varying(50),
  concept_type character varying(50),
  concept_class_id integer,
  concept_datatype_id integer
);
CREATE UNIQUE INDEX conceptname_idx ON concepts (longname);


-- Generate concepts from d_items table
insert into concepts (itemid,shortname,longname,concept_type,linksto) select itemid,label,concat(label,' [',concept_type,'_',itemid,']'),concat('test_',concept_type),linksto from (select itemid,label,unnest('{text,enum,num}'::text[]) concept_type,linksto,dbsource from d_items) c;


-- map distinct values for each item where avg occurance > 1000 
drop table if exists cetxt_map;
create table cetxt_map as select cetxt_tmp.value,summary.itemid from (select itemid,round(sum(num)/count(*)) density from cetxt_tmp group by itemid order by density) summary left join cetxt_tmp on cetxt_tmp.itemid = summary.itemid where summary.density > 1000 order by itemid,value;
-- add common chartevent text values as concepts
insert into concepts (longname,shortname,concept_type) select concat(value,' [common response]'),value,'charttext' from cetxt_map group by value order by value;


-- Process numeric chart data
-- summarize units
drop table if exists cenum_tmp_1;
create temporary table cenum_tmp_1 as select itemid,valueuom,count(*) from chartevents where valuenum is not null group by itemid,valueuom order by itemid;
-- summarize values
drop table if exists cenum_tmp_2;
create temporary table cenum_tmp_2 as select max(valuenum) max_val,min(valuenum) min_val,avg(valuenum) avg_val,itemid,count(*) from chartevents where valuenum is not null group by itemid order by itemid,count desc;
-- numeric questions grouped by itemid
drop table if exists cenum;
create table cenum as select cenum_tmp_2.itemid,min_val,avg_val,max_val,unitcounts.valueuom units,cenum_tmp_2.count num from cenum_tmp_2 left join (SELECT (mi).* FROM (SELECT  (SELECT mi FROM cenum_tmp_1 mi WHERE  mi.itemid = m.itemid ORDER BY count DESC LIMIT 1) AS mi FROM  cenum_tmp_1 m GROUP BY itemid) q ORDER BY  (mi).itemid) unitcounts on cenum_tmp_2.itemid = unitcounts.itemid;


-- diagnoses
insert into concepts (shortname,longname,concept_type) (select diagnosis,concat(diagnosis,' [diagnosis]'),'diagnosis' from admissions group by diagnosis);
-- noteevents categories
insert into concepts (shortname,longname,concept_type) (select category,concat(category,' [note category]'),'category' from noteevents group by category);

-- remove unneeded text concepts
--delete from concepts where concept_type  = 'test_text' and itemid in (select itemid from cetxt_map) and linksto = 'chartevents'; 
-- remove unneeded enum concepts
--delete from concepts where concept_type  = 'test_enum' and itemid not in (select itemid from cetxt_map) and linksto = 'chartevents'; 
--delete from concepts where concept_type = 'test_num' and itemid not in (select itemid from cenum) and linksto = 'chartevents';
delete from concepts where shortname is null;
update concepts set concept_class_id = 5, concept_datatype_id = 4 where concept_type  = 'charttext';
update concepts set concept_class_id = 4,  concept_datatype_id = 4 where concept_type  = 'diagnosis';
update concepts set concept_class_id = 7, concept_datatype_id = 3 where concept_type  = 'category';
update concepts set concept_class_id = 1, concept_datatype_id = 1 where concept_type  = 'test_num';
update concepts set concept_class_id = 1, concept_datatype_id = 2 where concept_type  = 'test_enum';
update concepts set concept_class_id = 1, concept_datatype_id = 3 where concept_type  = 'test_text';
