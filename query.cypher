WITH 'file:///share/devel/mrsman/data/Patient/fbe0ab72-ef35-459c-9b7b-7b32fb29819c.json' AS url
CALL apoc.load.json(url) YIELD value as b

WITH b, b.Encounter as encounters
//WITH b
MERGE (p:Patient {id:b.id, firstname:b.firstname, lastname:b.lastname, city:b.city, resourceType:'Patient'})
FOREACH (encounter in b.Encounter | 
    MERGE (c:Encounter {id:encounter.id, resourceType:'Encounter'})
    CREATE (p)-[:encountered]->(c)
    FOREACH (observation in encounter.Observation | 
        MERGE (o:Observation {id:observation.id, resourceType:'Observation'})
        MERGE (c)-[:spawned]->(o)
    )
    FOREACH (location in encounter.Location | 
        MERGE (l:Location {id:location.id, resourceType:'Location'})
        MERGE (c)-[:at]->(l)
    )
)



//WITH b.Encounter as es
//UNWIND es AS e
//MERGE (c:Encounter {id:e.id}) 
//ON CREATE SET c.resourceType = "Encounter"
//MERGE (p)-[:encountered]->(c)
RETURN count(*);




//MERGE (g:Encounter  {id:b.id, firstname:b.firstname, lastname:b.lastname, city:b.city})
//RETURN b;
//p.firstname = b.firstname, p.lastname = b.lastname, p.city = b.city
//MERGE (p:Patient {id:b.id}) ON CREATE SET p.firstname = b.firstname, p.lastname = b.lastname, p.city = b.city
//MERGE (c:Encounter {id:e.id}) ON CREATE SET c.resourceType = "Encounter"


//UNWIND p.Encounter AS e

//UNWIND e.Practitioner AS d
//UNWIND e.Observation AS o
//UNWIND e.Location AS l

//RETURN p.id AS patient_id,  e.id as encounter_id, l.id as location_id, d.id as practitioner_id, o.id as observation_id;
