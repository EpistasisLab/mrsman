MATCH (e:Encounter) where e.participant is not null with e MATCH (dr:Practitioner {uuid:e.participant}) create (e)<-[:participant]-(dr);
MATCH (e:Encounter) where e.location is not null with e MATCH (l:Location {uuid:e.location}) create (e)-[:location]->(l);
MATCH (e:Encounter) with e MATCH (p:Patient {uuid:e.patient}) create (e)-[:subject]->(p);
MATCH (e:Encounter) where e.partOf is not null with e MATCH (pe:Encounter {uuid:e.partOf}) create (e)-[:partOf]->(pe);
MATCH (o:Observations) where o.encounter_uuid is not null with o MATCH (e:Encounter {uuid:o.encounter_uuid}) create (o)-[:at]->(e);
MATCH (o:Observations) o MATCH (p:Patient {uuid:o.patient_uuid}) create (o)-[:of]->(p);
