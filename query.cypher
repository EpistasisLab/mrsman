WITH 'file:///share/devel/mrsman/example.json' AS url
CALL apoc.load.json(url) YIELD value as ll
UNWIND ll.Encounter AS x
UNWIND x.Observation AS y
RETURN x.id as encounter_id, ll.id AS patient_id, y.id as observation_id;
