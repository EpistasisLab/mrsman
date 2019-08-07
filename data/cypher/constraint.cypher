MATCH (o:chartevents)
SET o.uuid = o.observation_uuid
REMOVE o.observation_uuid;

MATCH (o:inputevents_cv)
WHERE o.observation_uuid IS NULL
SET o.uuid = o.observation_uuid
REMOVE o.observation_uuid;

MATCH (o:inputevents_mv)
WHERE o.observation_uuid IS NULL
SET o.uuid = o.observation_uuid
REMOVE o.observation_uuid;

MATCH (o:labevents)
WHERE o.observation_uuid IS NULL
SET o.uuid = o.observation_uuid
REMOVE o.observation_uuid;

MATCH (o:noteevents)
WHERE o.observation_uuid IS NULL
SET o.uuid = o.observation_uuid
REMOVE o.observation_uuid;

MATCH (o:outputevents)
WHERE o.observation_uuid IS NULL
SET o.uuid = o.observation_uuid
MATCH (o:procedureevents)
WHERE o.observation_uuid IS NULL
SET o.uuid = o.observation_uuid



CREATE CONSTRAINT ON (dr:Practitioner) ASSERT dr.uuid IS UNIQUE;

CREATE CONSTRAINT ON (p:Patient) ASSERT p.uuid IS UNIQUE;
CREATE CONSTRAINT ON (l:Location) ASSERT l.uuid IS UNIQUE;
CREATE CONSTRAINT ON (c:Concept) ASSERT c.uuid IS UNIQUE;
CREATE CONSTRAINT ON (e:Encounter) ASSERT e.uuid IS UNIQUE;
CREATE CONSTRAINT ON (t:Encountertype) ASSERT t.uuid IS UNIQUE;
CREATE CONSTRAINT ON (v:visittype) ASSERT v.uuid IS UNIQUE;



CREATE CONSTRAINT ON (d:DiagnosticReport) ASSERT d.uuid IS UNIQUE;
CREATE CONSTRAINT ON (o:chartevents) ASSERT o.observation_uuid IS UNIQUE;
CREATE CONSTRAINT ON (o:inputevents_cv) ASSERT o.observation_uuid IS UNIQUE;
CREATE CONSTRAINT ON (o:inputevents_mv) ASSERT o.observation_uuid IS UNIQUE;
CREATE CONSTRAINT ON (o:labevents) ASSERT o.observation_uuid IS UNIQUE;
CREATE CONSTRAINT ON (o:noteevents) ASSERT o.observation_uuid IS UNIQUE;
CREATE CONSTRAINT ON (o:outputevents) ASSERT o.observation_uuid IS UNIQUE;
CREATE CONSTRAINT ON (o:procedureevents) ASSERT o.observation_uuid IS UNIQUE;
