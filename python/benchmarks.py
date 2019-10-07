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
		'uuid':'566b762c-34a0-4ccc-b059-25250aa6886a',
		'units':'sec',
		'type':'numeric'
	},
	'Diastolic blood pressure':{
		'uuid':'5086AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
		'units':'mmHg',
		'type':'numeric'
        },
        'Fraction inspired oxygen':{
		'uuid':'6e1044de-2021-43ee-a845-eb8204560142',
		'units':'%',
		'type':'numeric'
	},
        'Glascow coma scale eye opening':{
		'uuid':'95eccf0e-adb0-4d00-bdcf-7315ddaecf2a',
		'units':'',
		'type':'text'
	},
        'Glascow coma scale motor response':{
		'uuid':'6b6c2d7c-8783-4914-9ca2-b5e3c2ac3cd1',
		'units':'',
		'type':'text'
	},
	'Glascow coma scale total':{
		'uuid':'6ab0e85a-3d9e-4e3d-94e3-c7e8f36acd66',
		'units':'',
		'type':'numeric'
	},
	'Glascow coma scale verbal response':{
		'uuid':'38ef5c7a-8c92-4033-aef5-d11325dff322',
		'units':'',
		'type':'text'
	},
	'Glucose':{
		'uuid':'3a90ffde-20e1-4eda-9e21-08d8b4c9e1ca',
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
		'uuid':'bf39005b-2996-44e9-8b1f-de2023e08112',
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
		'uuid':'ce58a9df-5e9e-4a0f-83c6-0e630bd0b637',
		'units':'',
		'type':'numeric'
	}
}



record = phenotyping.read_example(record_num)
patient_id=record['name'].split('_')[0]
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
patient_uuid = self.last_uuid
self.src = 'visits'
self.uuid = -1
self.filter = {'visits.subject_id':patient_id}
self.callback = mrsman.addSimpleAdmission
mrsman.runTask(self)
encounter_uuid = self.last_uuid



#patient_uuid = '4540c8ff-fff1-40c8-94aa-6d5e86dc50b7'
#encounter_uuid = '83a16c5b-a7b0-4db5-81d5-ec6d9b8382da'

list=record['X']
headers=record['header']
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
            #print(observation_uuid)
#            print(obs)
#            measurements[uuid] = rec;
        i+=1
#    for concept_uuid in measurements:
#        print(concept_uuid)
#        measurement = measurements[concept_uuid]
        #genFhirBenchmarkObs(self,patient_uuid,concept_uuid,encounter_uuid,value,units,value_type):
#    	print(meas)


