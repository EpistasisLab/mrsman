CALL apoc.load.json({url}) YIELD value as b
WITH b
MERGE (c:Concept {uuid:b.uuid, display:b.display, type:b.datatype.display , class:b.conceptClass.display})
RETURN count(*);
