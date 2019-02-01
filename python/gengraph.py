#!/usr/bin/env python3
from os import walk
import json
import py2neo
graph = py2neo.Graph(host="localhost", auth=("neo4j", "password")) 


#classnames = ["Patient","Practitioner","Encountertype","visittype","Location","Encounter","DiagnosticReport","Observation"]
classnames = ["Encounter"]

def importNode(uuid,classname):
    mypath='../data/json/'+classname
    filename = '../data/json/'+classname+'/'+uuid+'.json'
    cypherarray={
        "Patient":"with {json} as value unwind value.name as name merge (p:Practitioner {uuid:value.id, lastname:name.family, firstname:name.given[0], gender:value.gender, birthDate:value.birthDate})",
        "Practitioner":"with {json} as value unwind value.name as name merge (p:Patient {uuid:value.id, lastname:name.family, firstname:name.given[0], gender:value.gender, birthDate:value.birthDate, deceased:value.deceasedBoolean, openmrsID:value.identifier[0].value})",
        "Encountertype":"with {json} as value merge (e:Encountertype {uuid:value.id, name:value.name, description:value.description})",
        "visittype":"with {json} as value merge (v:visittype {uuid:value.id, name:value.name, description:value.description})",
        "Location":"with {json} as value merge (l:Location {uuid:value.id, name:value.name, description:value.description})",
        "Encounter":"with {json} as value with {uuid:value.id, patient:value.subject.id, location:split(value.location[0].location.reference,'/')[1],start:value.period.start,end:value.period.end,partOf:split(value.partOf.reference,'/')[1]} as enc merge (e:Encounter {uuid:enc.uuid}) on create set e.patient = enc.patient set e.start = enc.start, e.end = enc.end, e.location = enc.location, e.partOf = enc.partOf"
    }
    query = cypherarray[classname] 
    print(query)
    with open(filename) as data_file:
        j = json.load(data_file)
        j['id'] = uuid
        #print(query)
        graph.run(query, json = j)

for classname in classnames:
    print(classname)
    mypath='../data/json/'+classname
    f = []
    for (dirpath, dirnames, filenames) in walk(mypath):
        f.extend(filenames)
        break
    for filename in f:
        ext = filename.split(".")[1]
        base = filename.split(".")[0]
        if(ext == 'json'):
            importNode(base,classname)



#mypath='../data/json/Patient'
#f = []
#for (dirpath, dirnames, filenames) in walk(mypath):
#    f.extend(filenames)
#    break
#for filename in f:
#    ext = filename.split(".")[1]
#    base = filename.split(".")[0]
#    if(ext == 'json'):
#        importPatient(base)

#mypath='../data/json/Practitioner'
#f = []
#for (dirpath, dirnames, filenames) in walk(mypath):
#    f.extend(filenames)
#    break
#for filename in f:
#    ext = filename.split(".")[1]
#    base = filename.split(".")[0]
#    if(ext == 'json'):
#        importPractitioner(base)

