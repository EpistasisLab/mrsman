#!/usr/bin/env python3
from os import walk
import json
import py2neo
import queue
import threading
import time
import os
import sys
NEO4J_USER = os.environ['NEO4J_USER']
NEO4J_PASS = os.environ['NEO4J_PASS']
graph = py2neo.Graph(host="localhost", auth=(NEO4J_USER, NEO4J_PASS)) 

def runcfile(self):
    cfile = open('../data/cypher/constraint.cypher', "r") 
    for cypher  in cfile:
        graph.run(cypher)
    cfile.close()

def importNode(classname,uuid):
    mypath='../data/json/'+classname
    filename = '../data/json/'+classname+'/'+uuid+'.json'
    cypherarray={
#        "Practitioner":"with {json} as value unwind value.name as name merge (p:Practitioner {uuid:value.id, lastname:name.family, firstname:name.given[0], gender:value.gender, label:name.suffix[0], birthDate:value.birthDate})",
        "Practitioner":"with {json} as value unwind value.name as name merge (p:Practitioner {uuid:value.id, lastname:name.family, firstname:name.given[0], gender:value.gender, birthDate:value.birthDate})",
        "Patient":"with {json} as value unwind value.name as name merge (p:Patient {uuid:value.id, lastname:name.family, firstname:name.given[0], gender:value.gender, birthDate:value.birthDate, deceased:value.deceasedBoolean, openmrsID:value.identifier[0].value})",
        "Encountertype":"with {json} as value merge (e:Encountertype {uuid:value.id, name:value.name, description:value.description})",
        "visittype":"with {json} as value merge (v:visittype {uuid:value.id, name:value.name, description:value.description})",
        "Location":"with {json} as value merge (l:Location {uuid:value.id, name:value.name, description:value.description})",
        #"Encounter":"with {json} as value with {uuid:value.id, patient:value.subject.id, location:split(value.location[0].location.reference,'/')[1],start:value.period.start,end:value.period.end,partOf:split(value.partOf.reference,'/')[1]} as enc merge (e:Encounter {uuid:enc.uuid}) on create set e.patient = enc.patient, e.start = enc.start, e.end = enc.end, e.location = enc.location, e.partOf = enc.partOf"
        "Encounter":"with {json} as value with {uuid:value.id, patient:value.subject.id, location:split(value.location[0].location.reference,'/')[1],start:value.period.start,end:value.period.end,partOf:split(value.partOf.reference,'/')[1],participant:split(value.participant[0].individual.reference,'/')[1]} as enc merge (e:Encounter {uuid:enc.uuid}) on create set e.patient = enc.patient, e.start = enc.start, e.end = enc.end, e.location = enc.location, e.partOf = enc.partOf, e.participant = enc.participant"
    }
    query = cypherarray[classname] 
#    query = """
#        with {json} as value 
#        UNWIND value.participant AS participant
#        MERGE (c:Caregiver {c.uuid:participant.reference<F6>
#        MERGE (y:Year { year: event.year })
#        with {uuid:value.id, patient:value.subject.id, location:split(value.location[0].location.reference,'/')[1],start:value.period.start,end:value.period.end,partOf:split(value.partOf.reference,'/')[1]} as enc 
#        merge (e:Encounter {uuid:enc.uuid}) 
#        on create set e.patient = enc.patient, e.start = enc.start, e.end = enc.end, e.location = enc.location, e.partOf = enc.partOf
#    """
    with open(filename) as data_file:
        j = json.load(data_file)
        j['id'] = uuid
        #print(query)
        graph.run(query, json = j)


def loadGraph(self):
    exitFlag = 0
    class myThread (threading.Thread):
       def __init__(self, threadID, name, q):
          threading.Thread.__init__(self)
          self.threadID = threadID
          self.name = name
          self.q = q
       def run(self):
          print ("Starting " + self.name)
          process_data(self.name, self.q)
          print ("Exiting " + self.name)
    def process_data(threadName, q):
       while not exitFlag:
          queueLock.acquire()
          if not workQueue.empty():
             data = q.get()
             classname = data[0]
             uuid = data[1]
             queueLock.release()
             importNode(classname,uuid)
          else:
             queueLock.release()

#classnames = ["Location","Patient","Practitioner","Encountertype","visittype","Encounter","Observation"]
#    classnames = ["Location","Patient","Practitioner","Encountertype","visittype"]
#classnames = ["Location","Practitioner","Patient","Encountertype","visittype"]
#classnames = ["Location","Practitioner"]
#classnames = ["Encountertype","visittype"]
    classnames = ["Encounter"]
    threadList=["Thread_"+str(x) for x in range(50)]
    queueLock = threading.Lock()
    workQueue = queue.Queue()
    threads = []
    threadID = 1

# Create new threads
    for tName in threadList:
       thread = myThread(threadID, tName, workQueue)
       thread.start()
       threads.append(thread)
       threadID += 1

# Fill the queue
    queueLock.acquire()

    for classname in classnames:
        print(classname)
        mypath='../data/json/'+classname
        # This would print all the files and directories
        filelist = os.listdir( mypath )
        for filename in filelist:
            splitted = ext = filename.split(".")
            if(len(splitted) > 1 and splitted[1] == 'json'):
                base = splitted[0]
                workQueue.put([classname,base])

    queueLock.release()

    # Wait for queue to empty
    while not workQueue.empty():
       pass

    # Notify threads it's time to exit
    exitFlag = 1

# Wait for all threads to complete
    for t in threads:
       t.join()
    print ("Exiting Main Thread")
