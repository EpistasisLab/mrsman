#!/usr/bin/env python3
from neo4jrestclient import client
from neo4jrestclient.client import GraphDatabase
db = GraphDatabase("http://localhost:7474", username="neo4j", password="password")
import pandas as pd
 
#get patients with varying WHO HIV stage and their observations
q="MATCH (o:Observation {display:'CURRENT WHO HIV STAGE', value:'WHO STAGE 3 ADULT'})-[spawned]-(e:Encounter)-[encountered]-(p:Patient)-[]-(e2:Encounter)-[]-(ob:Observation  {display:'CURRENT WHO HIV STAGE'}) where not ob.value =  o.value  with p match (p)--(e3:Encounter)--(o3:Observation) Return distinct p.id as patient_id,e3.id as encounter_id,o3.display as observation,o3.value as value,o3.timestamp as timestamp order by p.id,o3.timestamp"
results = db.query(q, data_contents=True)
columns = results.columns;
df = pd.DataFrame(columns=columns)
stages = pd.DataFrame(columns = ['patient_id','timestamp','stage'])
last_patient_id = str(0);
last_timestamp = int(0);
for r in results:
    record = {}
    for i,val in enumerate(r):
        record[columns[i]] = val
    if (record['observation'] == 'CURRENT WHO HIV STAGE'):
        stages = stages.append(pd.DataFrame.from_records({'patient_id':record['patient_id'],'timestamp':record['timestamp'],'stage':record['value']},index=[0]))
    else:
        df = df.append(pd.DataFrame.from_records(record,index=[0]), ignore_index=True)
        if (record['patient_id'] != last_patient_id) | (record['timestamp'] != last_timestamp):
            osr = stages.query('patient_id == "'+ last_patient_id  +'" & timestamp == '+ str(last_timestamp))
            if(osr.shape[0] == 0):
                stages = stages.append(pd.DataFrame.from_records({'patient_id':last_patient_id,'timestamp':last_timestamp,'stage':'NO STAGE'},index=[0]))
    last_patient_id = record['patient_id']
    last_timestamp = record['timestamp']

keys = {}
lookup = pd.DataFrame(columns=['var','val','index'])
for var in columns:
    i = 0
    vals = df[var].unique()
    for val in vals:
        if(var == 'observation'):
          print(val);
        try:
          f=float(val)
        except ValueError:
          i += 1
          look = {'var':var,'val':val,'index':i}
          lookup = lookup.append(pd.DataFrame.from_records({'var':var,'val':val,'index':i},index=[0]))
    




df.to_pickle('observations.pkl')
stages.to_pickle('stages.pkl')
lookup.to_pickle('lookup.pkl')
