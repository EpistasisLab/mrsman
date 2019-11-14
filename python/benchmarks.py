#!/usr/bin/env python3
from os import walk
import json
import queue
import threading
import time
import os
import sys
import mrsman
import concurrent.futures
import asyncio
import os
from datetime import datetime, timedelta
sys.path.append('/data/devel/mimic3-benchmarks') 
from mimic3benchmark.readers import PhenotypingReader
phenotyping = PhenotypingReader(dataset_dir='/data/devel/mimic3-benchmarks/data/phenotyping/train',listfile='/data/devel/mimic3-benchmarks/data/phenotyping/train_listfile.csv')
num_threads = 50
exitFlag = False

class base ():
  def __init__(self):
      num_records = phenotyping.get_number_of_examples()
      assignments = retAssignments(num_threads,num_records)
      threads={}
      for x in range(0, num_threads):
        threads[x] = bmThread(x,self)
        threads[x].x = x
        threads[x].assignment = assignments[x]
      for x in threads:
        thread = threads[x]
        thread.start()
      for x in threads:
        thread = threads[x]
        thread.join()



class bmThread (threading.Thread):
   def __init__(self, threadID,parent):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = 'Thread' + str(threadID)
   def run(self):
      print ("Starting %s: %s" % (self.name, time.ctime(time.time())))
      mrsman.bootstrap(self)
      mrsman.getUuids(self)
      for record_num in self.assignment:
          print ("thread %s, record %s" % (self.name, record_num))
          retBenchmarkData(record_num,self)
          if exitFlag:
              break
          #if not (handleRecords(self)):
          #    break
          #self.counter -= 1
      print ("Exiting %s: %s" % (self.name, time.ctime(time.time())))



concepts = {
	'Capillary refill rate':{
#		'uuid':'5fa8e92d-5ebf-4e14-a182-a2e0bf3346de',
                'uuid': False,
		'units':'sec',
		'type':'numeric'
	},
	'Diastolic blood pressure':{
		'uuid':'5086AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
		'units':'mmHg',
		'type':'numeric'
        },
        'Fraction inspired oxygen':{
#		'uuid':'f06631dc-d734-4910-a566-c9451709fb4b',
                'uuid': False,
		'units':'%',
		'type':'numeric'
	},
        'Glascow coma scale eye opening':{
#		'uuid':'eb0d32e1-a7e1-41de-a2db-7e9d9b8135eb',
                'uuid': False,
		'units':'',
		'type':'text'
	},
        'Glascow coma scale motor response':{
#		'uuid':'eda80d82-19da-4ce9-bbd9-6e6b51d4e5af',
                'uuid': False,
		'units':'',
		'type':'text'
	},
	'Glascow coma scale total':{
#		'uuid':'82b7fa8d-200c-48e9-ad4a-744efda5d45f',
                'uuid': False,
		'units':'',
		'type':'numeric'
	},
	'Glascow coma scale verbal response':{
#		'uuid':'449f2538-f878-47a8-a255-082e6f1069df',
                'uuid': False,
		'units':'',
		'type':'text'
	},
	'Glucose':{
#		'uuid':'41f3312e-4c8f-4655-9016-ccadf2b5f521',
                'uuid': False,
		'units':'mmol/L',
		'type':'numeric'
	},
	'Heart Rate':{
		'uuid':'5087AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
		'units':'bpm',
		'type':'numeric'
	},
	'Height':{
		'uuid':'5090AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
		'units':'cm',
		'type':'numeric'
	},
	'Mean blood pressure':{
#		'uuid':'66d25bdc-1c1c-4464-87d1-fbc4d45870c9',
                'uuid': False,
		'units':'mmHg',
		'type':'numeric'
	},
	'Oxygen saturation':{
		'uuid':'5092AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
		'units':'%',
		'type':'numeric'
	},
	'Respiratory rate':{
		'uuid':'5242AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
		'units':'',
		'type':'numeric'
	},
	'Systolic blood pressure':{
		'uuid':'5085AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
		'units':'mmHg',
		'type':'numeric'
	},
	'Temperature':{
		'uuid':'5088AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
		'units':'DEG C',
		'type':'numeric'
	},
	'Weight':{
		'uuid':'5089AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
		'units':'kg',
		'type':'units'
	},
	'pH':{
#		'uuid':'4ffa7181-71a4-412b-8812-b25bc2a6aa47',
                'uuid': False,
		'units':'',
		'type':'numeric'
	}
}

def retAssignments(num_threads,num_records):
   assignments = {}
   for i in range(0,num_threads):
       assignments[i] = []
   for j in range(0,num_records):
       assignments[j % num_threads].append(j)
   return(assignments)


     


def retBenchmarkData(record_num,self):
    benchmark = phenotyping.read_example(record_num)
    patient_id=benchmark['name'].split('_')[0]
    benchmark_observations=benchmark['X']
    headers=benchmark['header']
    observations = []
    print("num_results: " + str(len(benchmark_observations)))
    for step in benchmark_observations: 
        measurements={}
        i=0
        date=str((datetime(2000, 1, 1, 0, 0) + timedelta(hours=float(step[0]))).isoformat())
        for value in step: 
            if(i >0 and len(value) > 0):
                concept_uuid = concepts[headers[i]]['uuid'];
                value_type = concepts[headers[i]]['type'];
                units = concepts[headers[i]]['units'];
                if(concept_uuid):
                    observation = {
                               'concept_uuid':concept_uuid,
                               'value_type':value_type,
                               'value':value,
                               'units':units,
                               'date':date
                    }
                    observations.append(observation)
            i+=1
    print("adding patient_id:"+patient_id)
#    self = type('mrsman', (object,), {})()
#    self.debug=True
#    self.fhir_array=[]
#    self.count = 1
    self.num = 1
    self.deltadate = True
    self.src = 'visits'
    self.uuid = 2
    self.observations = observations
    self.filter = {'visits.subject_id':patient_id}
    self.callback = mrsman.addVisitObservations
    mrsman.runTask(self)
#    print(self.fhir_array)

base()
