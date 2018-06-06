from neo4jrestclient import client
from datetime import datetime
from neo4jrestclient.client import GraphDatabase
db = GraphDatabase("http://127.0.0.2:7474", username="neo4j", password="password")
 
#get patients with varying WHO HIV stage and their observations
q = "MATCH (o:Observation) return distinct o.date"
results = db.query(q, data_contents=True)
for r in results:
    splitted = r[0].rsplit(':',1)
    datestring = splitted[0] + splitted[1];
    epoch = int(datetime.strptime(datestring, "%Y-%m-%dT%H:%M:%S%z").timestamp())
    q2 = "MATCH (o:Observation {date:\"" + r[0] + "\"}) set o.timestamp = " + str(epoch)
    db.query(q2, data_contents=True)
    print(q2)
#    print("(%s)-[%s]->(%s)" % (r[0]["name"], r[1], r[2]["name"]))
