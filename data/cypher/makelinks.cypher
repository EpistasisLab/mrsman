create index on :chartevents(encounter_uuid);
create index on :chartevents(caregiver_uuid);
create index on :chartevents(concept_uuid);
create index on :chartevents(patient_uuid);

create index on :diagnoses_icd(encounter_uuid);
create index on :diagnoses_icd(caregiver_uuid);
create index on :diagnoses_icd(concept_uuid);
create index on :diagnoses_icd(patient_uuid);

create index on :inputevents_cv(encounter_uuid);
create index on :inputevents_cv(caregiver_uuid);
create index on :inputevents_cv(concept_uuid);
create index on :inputevents_cv(patient_uuid);

create index on :inputevents_mv(encounter_uuid);
create index on :inputevents_mv(caregiver_uuid);
create index on :inputevents_mv(concept_uuid);
create index on :inputevents_mv(patient_uuid);

create index on :labevents(encounter_uuid);
create index on :labevents(caregiver_uuid);
create index on :labevents(concept_uuid);
create index on :labevents(patient_uuid);

create index on :noteevents(encounter_uuid);
create index on :noteevents(caregiver_uuid);
create index on :noteevents(concept_uuid);
create index on :noteevents(patient_uuid);

create index on :outputevents(encounter_uuid);
create index on :outputevents(caregiver_uuid);
create index on :outputevents(concept_uuid);
create index on :outputevents(patient_uuid);

MATCH (e:Encounter) where e.participant is not null with e MATCH (dr:Practitioner {uuid:e.participant}) create (e)<-[:participant]-(dr);
MATCH (e:Encounter) where e.location is not null with e MATCH (l:Location {uuid:e.location}) create (e)-[:location]->(l);
MATCH (e:Encounter) with e MATCH (p:Patient {uuid:e.patient}) create (e)-[:subject]->(p);
MATCH (e:Encounter) where e.partOf is not null with e MATCH (pe:Encounter {uuid:e.partOf}) create (e)-[:partOf]->(pe);



MATCH (o:labevents) with o MATCH (c:Concept {uuid:o.concept_uuid}) WHERE NOT o:conceptualized WITH o,c LIMIT 1000000 MERGE (o)-[:is]->(c) SET o:conceptualized RETURN COUNT(*) AS conceptualized



2f80a41c-c806-4920-9feb-911b75fc0be




CALL apoc.periodic.iterate(
"MATCH (c:Concept) with c match (o:labevents {concept_uuid:c.uuid}) return c,o",
"create (o)-[:issa]->(c) RETURN c,o",
 {batchSize:20, iterateList:true, parallel:true});

CALL apoc.periodic.iterate(
"MATCH (p:Patient) with p match (o {patient_uuid:p.uuid}) return p,o",
"create (o)-[:patient]->(p) RETURN p,o",
 {iterateList:true, parallel:true});

CALL apoc.periodic.iterate(
"MATCH (e:Encounter) with e match (o {encounter_uuid:e.uuid}) return e,o",
"create (o)-[:encounter]->(e) RETURN e,o",
{iterateList:true, parallel:true});



CALL apoc.periodic.iterate(
"MATCH (c:Concept) with c match (o:noteevents {concept_uuid:c.uuid}) return c,o",
"create (o)-[:issa]->(c) RETURN c,o",
 {batchSize:20, iterateList:true, parallel:true});



CALL apoc.periodic.iterate(
"MATCH (c:Concept) with c match (o:labevents {concept_uuid:c.uuid}) return c,o",
"create (o)-[:issa]->(c) RETURN c,o",
 {batchSize:20, iterateList:true, parallel:true});





MATCH (o:noteevents) with o MATCH (c:Concept {uuid:o.concept_uuid}) create (o)-[:isa]->(c);
MATCH (o:labevents) with o MATCH (c:Concept {uuid:o.concept_uuid}) create (o)-[:isa]->(c);



MATCH (o:labevents) o MATCH (p:Patient {uuid:o.patient_uuid}) create (o)-[:of]->(p);

MATCH (o:noteevents) with o MATCH (c:Practitioner {uuid:o.caregiver_uuid}) create (c)-[:created]->(o);
MATCH (o:labevents) with o MATCH (c:Practitioner {uuid:o.caregiver_uuid}) create (c)-[:created]->(o);
MATCH (o:outputevents) with o MATCH (c:Practitioner {uuid:o.caregiver_uuid}) create (c)-[:created]->(o);
MATCH (o:procedureevents_mv) with o MATCH (c:Practitioner {uuid:o.caregiver_uuid}) create (c)-[:created]->(o);
MATCH (o:inputevents_cv) with o MATCH (c:Practitioner {uuid:o.caregiver_uuid}) create (c)-[:created]->(o);
MATCH (o:inputevents_mv) with o MATCH (c:Practitioner {uuid:o.caregiver_uuid}) create (c)-[:created]->(o);
MATCH (o:noteevents) with o MATCH (e:Encounter {uuid:o.encounter_uuid}) create (o)-[:during]->(e);
MATCH (o:labevents) with o MATCH (e:Encounter {uuid:o.encounter_uuid}) create (o)-[:during]->(e);
MATCH (o:outputevents) with o MATCH (e:Encounter {uuid:o.encounter_uuid}) create (o)-[:during]->(e);
MATCH (o:procedureevents_mv) with o MATCH (e:Encounter {uuid:o.encounter_uuid}) create (o)-[:during]->(e);
MATCH (o:procedureevents_cv) with o MATCH (e:Encounter {uuid:o.encounter_uuid}) create (o)-[:during]->(e);
MATCH (o:procedureevents_mv) with o MATCH (e:Encounter {uuid:o.encounter_uuid}) create (o)-[:during]->(e);
MATCH (o:noteevents) with o MATCH (c:Practitioner {uuid:o.caregiver_uuid}) create (c)-[:created]->(o);



MATCH (o:noteevents) with o MATCH (c:Practitioner {uuid:o.caregiver_uuid}) create (c)-[:created]->(o);
MATCH (o:labevents) with o MATCH (c:Practitioner {uuid:o.caregiver_uuid}) create (c)-[:created]->(o);
MATCH (o:outputevents) with o MATCH (c:Practitioner {uuid:o.caregiver_uuid}) create (c)-[:created]->(o);
MATCH (o:procedureevents_mv) with o MATCH (c:Practitioner {uuid:o.caregiver_uuid}) create (c)-[:created]->(o);
MATCH (o:inputevents_cv) with o MATCH (c:Practitioner {uuid:o.caregiver_uuid}) create (c)-[:created]->(o);
MATCH (o:inputevents_mv) with o MATCH (c:Practitioner {uuid:o.caregiver_uuid}) create (c)-[:created]->(o);
MATCH (o:noteevents) with o MATCH (e:Encounter {uuid:o.encounter_uuid}) create (o)-[:during]->(e);
MATCH (o:labevents) with o MATCH (e:Encounter {uuid:o.encounter_uuid}) create (o)-[:during]->(e);
MATCH (o:outputevents) with o MATCH (e:Encounter {uuid:o.encounter_uuid}) create (o)-[:during]->(e);
MATCH (o:procedureevents_mv) with o MATCH (e:Encounter {uuid:o.encounter_uuid}) create (o)-[:during]->(e);
MATCH (o:procedureevents_cv) with o MATCH (e:Encounter {uuid:o.encounter_uuid}) create (o)-[:during]->(e);
MATCH (o:procedureevents_mv) with o MATCH (e:Encounter {uuid:o.encounter_uuid}) create (o)-[:during]->(e);

