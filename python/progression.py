#!/usr/bin/env python3
from neo4jrestclient import client
from neo4jrestclient.client import GraphDatabase
db = GraphDatabase("http://localhost:7474", username="neo4j", password="password")
import pandas as pd
 
print("query observations")
#get all observations from patients with varying stage and their observations
#q="MATCH (o:Observation {display:'CURRENT WHO HIV STAGE', value:'WHO STAGE 3 ADULT'})-[spawned]-(e:Encounter)-[encountered]-(p:Patient)-[]-(e2:Encounter)-[]-(ob:Observation  {display:'CURRENT WHO HIV STAGE'}) where not ob.value =  o.value  with p match (p)--(e3:Encounter)--(o3:Observation) Return distinct p.id as patient_id,e3.id as encounter_id,o3.display as observation,o3.value as value,o3.timestamp as timestamp order by p.id,o3.timestamp"
#get all observations from patients with any stage and their observations
q="MATCH (o:Observation {display:'CURRENT WHO HIV STAGE'})-[spawned]-(e:Encounter)-[encountered]-(p:Patient)-[encounter]-(e2:Encounter)-[]-(o2:Observation) Return p.id as patient_id,e2.id as encounter_id,o2.display as observation,o2.value as value,o2.timestamp as timestamp order by p.id,o2.timestamp limit 10000"

results = db.query(q, data_contents=True)
columns = results.columns;
obs = pd.DataFrame(columns=columns)
stages = {}
print("processing observations")
for r in results:
    record = {}
    for i,val in enumerate(r):
        record[columns[i]] = val
    patient_id = record['patient_id']
    timestamp = record['timestamp']
    obs = obs.append(pd.DataFrame.from_records(record,index=[0]), ignore_index=True)
    if (record['observation'] == 'CURRENT WHO HIV STAGE'):
        try:
            stage = int(record['value'][10])
        except:
            stage = 0
        if patient_id not in stages.keys():
           stages[patient_id] = {}
        if timestamp not in stages[patient_id].keys():
           stages[patient_id][timestamp] = stage
        #handle duplicates: use whatever stage isn't zero, otherwise comma sep
        elif stage != stages[patient_id][timestamp]:
           if stage == 0 & stages[patient_id][timestamp] != 0:
             newstage = stages[patient_id][timestamp]
           elif stage != 0 & stages[patient_id][timestamp] == 0:
             newstage = stage
           else:
             newstage = int(str(stage) + str(stages[patient_id][timestamp]))
           stages[patient_id][timestamp] = newstage
    #else:
    #    df = df.append(pd.DataFrame.from_records(record,index=[0]), ignore_index=True)

print("create lookup table")
lu = {}
for var in columns:
    i = 0
    vals = obs[var].unique()
    lu[var] = {}
    for val in vals:
        try:
          f=float(val)
        except ValueError:
          i += 1
          lu[var][val] = i
    

def getstage(rec):
   patient_id = rec.patient_id
   timestamp = rec.timestamp
   if patient_id not in stages.keys():
     stage = 0
   elif timestamp not in stages[patient_id]:
     stage = 0
   else:
     stage = stages[patient_id][timestamp] 
   return stage


print("analyze staging")
observations = obs.copy();
observations['stage'] = 0;
for i, row in observations.iterrows():
  stage = getstage(row)
  observations.at[i,'stage'] = stage
  for column in columns:
    try: 
      val = lu[column][row[column]]
    except:
      val = row[column]
    observations.at[i,column] = val




print("saving observations")
observations.to_csv('data/csv/observations.csv',index_label=False)
observations.to_pickle('data/pickle/observations.pkl')
print("fin")
