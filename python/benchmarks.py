#!/usr/bin/env python3
from os import walk
import json
import queue
import threading
import time
import os
import sys
import mrsman
from datetime import datetime, timedelta
sys.path.append('/data/devel/mimic3-benchmarks') 
from mimic3benchmark.readers import PhenotypingReader
phenotyping = PhenotypingReader(dataset_dir='/data/devel/mimic3-benchmarks/data/phenotyping/train',listfile='/data/devel/mimic3-benchmarks/data/phenotyping/train_listfile.csv')
print(sys.argv[1])

if(sys.argv[1].isdigit()):
    record_num = int(eval(sys.argv[1]))
else:
    record_num = 21

concept_uuids = {
	'Capillary refill rate':{
		'uuid':'',
		'units':'sec',
		'type':'numeric'
	},
	'Diastolic blood pressure':{
		'uuid':'5086AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
		'units':'mmHg',
		'type':'numeric'
        },
        'Fraction inspired oxygen':{
		'uuid':'',
		'units':'%',
		'type':'numeric'
	},
        'Glascow coma scale eye opening':{
		'uuid':'',
		'units':'',
		'type':'text'
	},
        'Glascow coma scale motor response':{
		'uuid':'',
		'units':'',
		'type':'text'
	},
	'Glascow coma scale total':{
		'uuid':'',
		'units':'',
		'type':'numeric'
	},
	'Glascow coma scale verbal response':{
		'uuid':'',
		'units':'',
		'type':'text'
	},
	'Glucose':{
		'uuid':'',
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
		'uuid':'',
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
		'uuid':'',
		'units':'',
		'type':'numeric'
	}
}



benchmark = phenotyping.read_example(record_num)
patient_id=benchmark['name'].split('_')[0]
print("adding patient_id:"+patient_id)
self = type('mrsman', (object,), {})()
self.debug=True
mrsman.bootstrap(self)
mrsman.getUuids(self)
self.deltadate = True
self.src = 'patients'
self.uuid = -1
self.callback = mrsman.addPatient
self.debug = True
mrsman.numThreads = 1
self.num=1
self.filter = {'patients.subject_id':patient_id}
mrsman.runTask(self)
#finished adding patient
patient_uuid = self.last_uuid
self.src = 'visits'
self.uuid = -1
self.filter = {'visits.subject_id':patient_id}
self.callback = mrsman.addSimpleAdmission
mrsman.runTask(self)
encounter_uuid = self.last_uuid

#unpack mimic-benchmarks processed record
list=benchmark['X']
headers=benchmark['header']
observations = []
for step in list: 
    measurements={}
    i=0
    date=str((datetime(2000, 1, 1, 0, 0) + timedelta(hours=float(step[0]))).isoformat())
    for value in step: 
        if(i >0 and len(value) > 0):
            concept_uuid = concept_uuids[headers[i]]['uuid'];
            value_type = concept_uuids[headers[i]]['type'];
            units = concept_uuids[headers[i]]['units'];
            obs = mrsman.genFhirBenchmarkObs(self,patient_uuid,concept_uuid,encounter_uuid,date,value,units,value_type)
            observation_uuid = mrsman.postDict('fhir', 'Observation', obs)
        i+=1


mrsman.bootstrap(self)
self.deltadate = True
self.uuid = 2
self.src = 'visits'
self.callback = mrsman.addMbEvents
self.filter = {'uuid':encounter_uuid}
mrsman.runTask(self)
