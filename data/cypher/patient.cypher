CALL apoc.load.json({url}) YIELD value as b
WITH b, b.Encounter as encounters
MERGE (p:Patient {id:b.id, firstname:b.firstname, lastname:b.lastname, city:b.city, birthdate: b.birthDate, gender: b.gender, deceased: b.deceasedBoolean})
FOREACH (encounter in b.Encounter | 
    MERGE (c:Encounter {id:encounter.id})
    MERGE (p)-[:encountered]->(c)
    FOREACH (observation in encounter.Observation | 
        MERGE (o:Observation {id:observation.id})
        SET o.display = observation.display, o.value=observation.value, o.unit=observation.unit, o.date=observation.date, o.concept = observation.concept
        MERGE (c)-[:spawned]->(o)
    )
    FOREACH (practitioner in encounter.Practitioner | 
        MERGE (dr:Practitioner {id:practitioner.id})
        MERGE (c)-[:by]->(dr)
    )
    FOREACH (location in encounter.Location | 
        MERGE (l:Location {id:location.id})
        MERGE (c)-[:at]->(l)
    )
)
RETURN count(*);
