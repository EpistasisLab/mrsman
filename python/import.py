#!/usr/bin/env python3
import psycopg2
import psycopg2.extras
import pymysql
import uuid
import time
import json
import names
import requests
import random
import time
from luhn import *
from metadata import *
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree
import sys
import math
import os
from datetime import date
from dateutil.relativedelta import relativedelta
debug = False
use_omrsnum = False

# UTILITY
#
#write dictionary to a file
def save_json(model_type,uuid,data):
    json_path = '/data/devel/mrsman/data/json'
    directory = json_path + '/' + model_type
    filename = directory + '/' + uuid + '.json'
    if not os.path.exists(directory):
        os.makedirs(directory) 
    with open(filename, 'w') as outfile:
        #print('writing ' + filename)
        json.dump(data, outfile)

def read_config():
    parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(sys.argv[0])))
    with open(parent_dir + '/config.json') as f:
        global config
        data = json.load(f)
        config = data['global']
        config['baseuri'] = 'http://' + config['IP'] + ':' +  config['OPENMRS_PORT'] + '/openmrs/ws'


# choose a random date from a range
def randomDate(start, end):
    date_format = '%Y-%m-%d'
    prop = random.random()
    stime = time.mktime(time.strptime(start, date_format))
    etime = time.mktime(time.strptime(end, date_format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(date_format, time.localtime(ptime)) + 'T00:00:00'

# generate a luhn mod 30 check character 
def luhnmod30(id_without_check):
    mod = 30
    # allowable characters within identifier
    valid_chars = '0123456789ACDEFGHJKLMNPRTUVWXY'
    # remove leading or trailing whitespace, convert to uppercase
    id_without_checkdigit = id_without_check.strip().upper()
    # this will be a running total
    sum = 0
    # loop through digits from right to left
    for n, char in enumerate(reversed(id_without_checkdigit)):
        if not valid_chars.count(char):
            raise Exception('InvalidIDException')
        # our "digit" is calculated using ASCII value - 48
        digit = valid_chars.find(char)
        digit = ord(char) - 48
        # weight will be the current digit's contribution to
        # the running total
        weight = None
        if (n % 2 == 0):
            weight = (2 * digit)
        else:
            # even-positioned digits just contribute their value
            weight = digit
        # keep a running total of weights
        sum += weight
    sum = math.fabs(sum) + mod
    # check digit is amount needed to reach next number
    # divisible by ten. Return an integer
    checkdigit =  int((mod - (sum % mod)) % mod)
    return  valid_chars[checkdigit]

#shift dates by offset number of hours
def deltaDate(src_date, offset):
    return str((src_date + relativedelta(days=-offset)).isoformat())

#open a postgres cursor set to sister name
def openPgCursor():
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
#    pg_cur.execute("SET search_path TO " + config['SISTER'])
    return(pg_cur)

# DATA I/O
#
#load not-yet-imported mimic records
def getSrc(table, limit):
    pg_cur = openPgCursor()
    stmt = "select * from " + table + " where row_id not in (select row_id from uuids where src = '" + table + "')"
    if (limit):
        stmt += " limit " + limit
    try:
        pg_cur.execute(stmt)
        return pg_cur
    except Exception as e:
        print("can't select from " + table)
        print(e)
        exit()

#load not-yet-imported mimic records joined to deltadate
def getSrcDelta(table, limit):
    pg_cur = openPgCursor()
    stmt = "select " + table + ".*,deltadate.offset from " + table + " left join deltadate on deltadate.subject_id = " + table + ".subject_id where row_id not in (select row_id from uuids where src = '" + table + "')"
    if (limit):
        stmt += " limit " + limit
    try:
        if debug:
            print(stmt)
        pg_cur.execute(stmt)
        return pg_cur
    except Exception as e:
        print("can't select from " + table)
        print(e)
        exit()

#load imported mimic records with filter
def getSrcUuid(table, Dict):
    pg_cur = openPgCursor()
    stmt = "select " + table + ".*,uuids.uuid from " + table + " left join uuids on " + table + ".row_id = uuids.row_id and uuids.src = '" + table + "'"
    if Dict:
        for col_name in Dict:
            stmt += " and " + table + "." + col_name + " = '" + str(
                Dict[col_name]) + "'"
    try:
        if debug:
            print(stmt)
        pg_cur.execute(stmt)
        return pg_cur
    except Exception as e:
        print("can't select from " + table)
        print(e)
        exit()

#get enumerated concepts
def getConceptMap():
    pg_cur = openPgCursor()
    stmt = "select concepts.openmrs_id parent_id,cm.openmrs_id child_id from (select cetxt_map.itemid,concepts.openmrs_id from cetxt_map left join concepts on cetxt_map.value = concepts.shortname and concepts.concept_type = 'answer') cm left join concepts on cm.itemid = concepts.itemid and concepts.concept_type = 'test_enum'"
    try:
        pg_cur.execute(stmt)
        return pg_cur
    except Exception as e:
        print("can't select from " + table)
        print(e)
        exit()

#load all locations into an array for easy searching
def getLocations():
    cur = getSrcUuid('locations', None)
    locations = {}
    for location in cur:
        locations[location.location] = location.uuid
    return (locations)

#load all caregivers into an array for easy searching
def getCaregivers():
    cur = getSrcUuid('mimiciii.caregivers', None)
    caregivers = {}
    for caregiver in cur:
        caregivers[caregiver.cgid] = caregiver.uuid
    return(caregivers)

#load all concepts into an array for easy searching
def getConcepts():
    cur = getSrcUuid('concepts', None)
    concepts = {}
    concepts['test_num'] = {}
    concepts['test_text'] = {}
    concepts['test_enum'] = {}
    concepts['diagnosis'] = {}
    concepts['answer'] = {}
    concepts['category'] = {}
    concepts['icd9_codes'] = {}
    for concept in cur:
        if concept.concept_type in ['test_num','test_text','test_enum']:
            concepts[concept.concept_type][concept.itemid] = concept.uuid
        elif concept.concept_type in ['diagnosis','answer','category']:
            concepts[concept.concept_type][concept.shortname] = concept.uuid
        elif concept.concept_type in ['icd_diagnosis','icd_procedure']:
            concepts['icd9_codes'][concept.icd9_code] = concept.uuid
    return concepts

#load all caregivers into an array for easy searching
def getdItems():
    cur = getSrcUuid('d_items', None)
    ditems = {}
    for ditem in cur:
        ditems[ditem.itemid] = ditem.uuid
    return(ditems) 

#load mimic records with filter
def getSrcFilter(table, Dict):
    pg_cur = openPgCursor()
    #pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    stmt = "select * from " + table
    for col_name in Dict:
        stmt += " where " + col_name + " = '" + str(Dict[col_name]) + "'"
    try:
        if debug:
            print(stmt)
        pg_cur.execute(stmt)
        return pg_cur
    except Exception as e:
        print("can't select from " + table)
        print(e)
        exit()

#create record in openmrs database
def insertDict(table, Dict):
    placeholder = ", ".join(["%s"] * len(Dict))
    mysql_cur = mysql_conn.cursor()
    stmt = "insert into `{table}` ({columns}) values ({values});".format(
        table=table, columns=",".join(Dict.keys()), values=placeholder)
    try:
        mysql_cur.execute(stmt, list(Dict.values()))
        rowid = mysql_cur.lastrowid
        mysql_cur.close()
        return rowid
    except Exception as e:
        print("can't insert into  " + table)
        print(e)
        print('failed record')
        print(Dict)
        mysql_cur.close()
        exit()

#set concepts auto_increment value
def setIncrementer(table,value):
    mysql_cur = mysql_conn.cursor()
    stmt = "ALTER TABLE "+table+" AUTO_INCREMENT = " + value
    try:
        mysql_cur.execute(stmt)
        mysql_cur.close()
        return True
    except Exception as e:
        print("can't set auto_increment")
        return False
        mysql_cur.close()
        exit()

#create record in mimic database
def updatePgDict(table, Dict, Filter):
    pg_cur = openPgCursor()
    placeholder = ", ".join(["%s"] * len(Dict))
    stmt = "update {table} set ({columns}) = ROW({values})".format(
        table=table, columns=",".join(Dict.keys()), values=placeholder)
    for col_name in Filter:
        stmt += " where " + col_name + " = '" + str(Filter[col_name]) + "'"
    try:
        pg_cur.execute(stmt, list(Dict.values()))
        return pg_cur
    except Exception as e:
        print("can't update " + table)
        print(e)
        exit()

#create record in postgres database
def insertPgDict(table, Dict):
    #pg_cur = pg_conn.cursor()
    pg_cur = openPgCursor()
    placeholder = ", ".join(["%s"] * len(Dict))
    stmt = "insert into {table} ({columns}) values ({values});".format(
        table=table, columns=",".join(Dict.keys()), values=placeholder)
    try:
        pg_cur.execute(stmt, list(Dict.values()))
        return pg_cur
    except Exception as e:
        print("can't insert into  " + table)
        print(e)
        exit()

#run sql from file in mimic database
def loadPgsqlFile(filename):
    pg_cur = openPgCursor()
    #pg_cur = pg_conn.cursor()
    try:
        pg_cur.execute(open(filename, "r").read())
        pg_conn.commit()
        return pg_cur
    except Exception as e:
        print("can't load file")
        print(e)
        exit()

#post a json encoded record to the fhir/rest interface
def postDict(endpoint, table, Dict):
    if (endpoint == 'fhir'):
        uri = config['baseuri'] + "/fhir/" + table.capitalize()
    else:
        uri = config['baseuri'] + "/rest/v1/" + table
    r = requests.post(uri, json=Dict, auth=HTTPBasicAuth(config['OPENMRS_USER'], config['OPENMRS_PASS']))
    if debug:
        print('post:')
        print(Dict)
        print('response:')
        print(r)
    if ("Location" in r.headers):
        uuid = r.headers['Location'].split('/').pop()
        #save_json(table,uuid,Dict)
        return (uuid)
    else:
        response = json.loads(r.text)
        if ('uuid' in response):
            uuid = response['uuid']
            #save_json(table,uuid,Dict)
            return(uuid)
        else:
            print("Unexpected response:")
            print(r.text)
            print("Dict:")
            print(Dict)
        return(False)

#post a json encoded record to the fhir/rest interface
def putDict(endpoint, table, Dict):
    new_uuid = str(uuid.uuid4())
    Dict['id'] = new_uuid;
    if (endpoint == 'fhir'):
        uri = config['baseuri'] + "/fhir/" + table.capitalize() + "/" + new_uuid
    else:
        uri = config['baseuri']  + "/rest/v1/" + table
    r = requests.put(uri, json=Dict, auth=HTTPBasicAuth(config['OPENMRS_USER'], config['OPENMRS_PASS']))
    if ("Location" in r.headers):
        return (r.headers['Location'].split('/').pop())
    else:
        print("Unexpected response:")
        print(r.text)
        return(False)

#load not-yet-imported admissions records for imported patients
def getAdmissions(limit):
    pg_cur = openPgCursor()
    #pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    stmt = "select hadm_id,a.row_id,visittype_uuids.uuid visit_type_uuid,discharge_location_uuids.uuid discharge_location_uuid,admission_location_uuids.uuid admission_location_uuid,patient_uuid,admittime,dischtime,admission_type,visittypes.row_id visit_type_code,admission_location,discharge_location,diagnosis,edregtime,edouttime,deltadate.offset from mimiciii.admissions a left join (select uuid patient_uuid,patients.* from mimiciii.patients left join uuids on mimiciii.patients.row_id = uuids.row_id where uuids.src = 'mimiciii.patients') p  on a.subject_id = p.subject_id left join locations admission_locations on a.admission_location = admission_locations.location left join locations discharge_locations on a.discharge_location = discharge_locations.location left join uuids admission_location_uuids on admission_locations.row_id = admission_location_uuids.row_id  and admission_location_uuids.src = 'locations' left join uuids discharge_location_uuids on discharge_locations.row_id = discharge_location_uuids.row_id  and discharge_location_uuids.src = 'locations' left join visittypes on a.admission_type = visittypes.visittype left join uuids visittype_uuids on visittype_uuids.row_id = visittypes.row_id and visittype_uuids.src = 'visittypes' left join deltadate on deltadate.subject_id = p.subject_id where patient_uuid is not null and a.row_id not in (select row_id from uuids where src = 'mimiciii.admissions')"
    if (limit):
        stmt += " limit " + limit
    try:
        pg_cur.execute(stmt)
        return pg_cur
    except Exception as e:
        print("can't load admissions")
        print(e)
        exit()

# load data from admission related tables
def getAdmissionData(admission):
    events_tables = [
        'chartevents','cptevents','datetimeevents','labevents','inputevents_cv',
        'inputevents_mv','labevents','microbiologyevents','noteevents',
        'outputevents','procedureevents_mv','procedures_icd'
    ]
    tables = [
        'callout','diagnoses_icd','drgcodes','icustays','prescriptions',
        'services','transfers']
    admission_data = {}
    for table in tables:
        admission_data[table] = []
        cur = getSrcFilter('mimiciii.' + table, {'hadm_id': admission.hadm_id})
        for record in cur:
            admission_data[table].append(record)
    admission_data['events'] = {}
    for table in events_tables:
        admission_data['events'][table] = []
        cur = getSrcFilter('mimiciii.' + table, {'hadm_id': admission.hadm_id})
        for record in cur:
            admission_data['events'][table].append(record)

    return (admission_data)

def addNote(admission,note,parent_uuid):
    concept_uuid = notecategory_array[note.category]
    observation = {
       "resourceType": "Observation",
        "code": {
            "coding": [{
                # "display": "Text of encounter note"
                "system": "http://openmrs.org",
                "code": concept_uuid
            }]
        },
        "subject": {
            "id": admission.patient_uuid,
        },
        "effectiveDateTime": deltaDate(note.chartdate, admission.offset),
        "issued": deltaDate(note.chartdate, admission.offset),
        "valueString": note.text,
            "context": {
            "reference": "Encounter/" + parent_uuid,
        }
    }
    if(note.cgid):
        cg_uuid = caregiver_array[note.cgid];
        performer = [
             {
              "reference": "Practitioner/"+cg_uuid,
             }
        ]
        observation['performer'] = performer;
    observation_uuid = postDict('fhir', 'observation', observation)
    return(observation_uuid)

def addtxtChart(admission,chart,parent_uuid):
    concept_uuid = ditems_array[chart.itemid]
    observation = {
       "resourceType": "Observation",
        "code": {
            "coding": [{
                # "display": "Text of encounter note"
                "system": "http://openmrs.org",
                "code": concept_uuid
            }]
        },
        "subject": {
            "id": admission.patient_uuid,
        },
        "effectiveDateTime": deltaDate(chart.charttime, admission.offset),
        "issued": deltaDate(chart.charttime, admission.offset),
        "value": chart.value,
            "context": {
            "reference": "Encounter/" + parent_uuid,
        }
    }
    if(chart.cgid):
        cg_uuid = caregiver_array[chart.cgid];
        performer = [
             {
              "reference": "Practitioner/"+cg_uuid,
             }
        ]
        observation['performer'] = performer;
    observation_uuid = postDict('fhir', 'observation', observation)
    return(observation_uuid)

def addnumChart(admission,chart,parent_uuid):
    concept_uuid = ditems_array[chart.itemid]
    observation = {
       "resourceType": "Observation",
        "code": {
            "coding": [{
                # "display": "Text of encounter note"
                "system": "http://openmrs.org",
                "code": concept_uuid
            }]
        },
        "subject": {
            "id": admission.patient_uuid,
        },
        "effectiveDateTime": deltaDate(chart.charttime, admission.offset),
        "issued": deltaDate(chart.charttime, admission.offset),
        "valueQuantity": {
            "value": chart.valuenum,
            "unit": chart.valueuom,
            "system": "http://unitsofmeasure.org",
        },
            "context": {
            "reference": "Encounter/" + parent_uuid,
        }
    }
    if(chart.cgid):
        cg_uuid = caregiver_array[chart.cgid];
        performer = [
             {
              "reference": "Practitioner/"+cg_uuid,
             }
        ]
        observation['performer'] = performer;
    observation_uuid = postDict('fhir', 'observation', observation)
    return(observation_uuid)


def addLab(admission,lab,encounter_uuid):
    concept_uuid = concepts_array['test_num'][lab.itemid]
    if(lab.valuenum and lab.valueuom):
      observation = {
       "resourceType": "Observation",
        "code": {
            "coding": [{
                "system": "http://openmrs.org",
                "code": concept_uuid
            }]
        },
        "subject": {
            "id": admission.patient_uuid,
        },
        "effectiveDateTime": deltaDate(lab.charttime, admission.offset),
        "issued": deltaDate(lab.charttime, admission.offset),
        "valueQuantity": {
            "value": lab.valuenum,
            "unit": lab.valueuom,
            "system": "http://unitsofmeasure.org",
        },
            "context": {
            "reference": "Encounter/" + encounter_uuid,
        }
    }
      observation_uuid = postDict('fhir', 'observation', observation)
      return(observation_uuid)
    else:
      return False

#MODEL MANIPULATION
#
# insert visit type records into encountertypes table in openmrs db
def visittypestoVisitTypes():
    src = 'visittypes'
    et_cur = getSrc(src, None)
    for record in et_cur:
        date = time.strftime('%Y-%m-%d %H:%M:%S')
        et_uuid = str(uuid.uuid4())
        et_id = insertDict(
            'visit_type', {
                "creator": "1",
                "uuid": et_uuid,
                "description": record.visittype,
                "name": record.visittype,
                "date_created": date,
            })
        uuid_cur = insertPgDict('uuids', {
            'src': src,
            'row_id': record.row_id,
            'uuid': et_uuid
        })
        uuid_cur.close()
    et_cur.close()
    pg_conn.commit()

# post practitioners to openmrs fhir interface
def caregiversToPractitioners(limit):
    src = 'mimiciii.caregivers'
    concept_cur = getSrc(src, limit)
    for record in concept_cur:
        birthdate = randomDate("1900-01-01", "2000-01-01")
        gender = random.choice(['male', 'female'])
        uuid = postDict(
            'fhir',
            'practitioner', {
                "resourceType": "Practitioner",
                "name": [{
                    "family": names.get_last_name(),
                    "given": [names.get_first_name(gender=gender)],
                    "suffix": [record.label]
                }],
                "gender": gender,
                "birthDate": birthdate,
                "active": True
            })
        if uuid:
            uuid_cur = insertPgDict('uuids', {
                'src': src,
                'row_id': record.row_id,
                'uuid': uuid
            })
            uuid_cur.close()
            pg_conn.commit()
        else:
            print("Caregiver not created")
            pg_conn.commit()
            exit()
    concept_cur.close()

# post patients to openmrs fhir interface
def patientsToPatients(limit):
    src = 'mimiciii.patients'
    patient_cur = getSrcDelta(src, limit)
    for record in patient_cur:
        gender = {"M": "male", "F": "female"}[record.gender]
        deceasedBoolean = {1: True, 0: False}[record.expire_flag]
        if(use_omrsnum):
            OpenMRSIDnumber = str(record.subject_id) + '-' + str(
                generate(str(record.subject_id)))
            identifier = [{
                "system": "OpenMRS Identification Number",
                "use": "usual",
                "value": OpenMRSIDnumber
            }]
        else:
            OpenMRSID = str(record.subject_id) + str(
                luhnmod30(str(record.subject_id)))
            identifier = [{
                "use": "usual",
                "system": "OpenMRS ID",
                "value": OpenMRSID
            }]
        patient = {
            "resourceType":
            "Patient",
            "identifier": identifier,
            "name": [{
                "use": "usual",
                "family": names.get_last_name(),
                "given": [names.get_first_name(gender=gender)]
            }],
            "gender":
            gender,
            "birthDate":
            deltaDate(record.dob, record.offset),
            "deceasedBoolean":
            deceasedBoolean,
            "active":
            True
        }
        print(patient)
        uuid = postDict('fhir', 'patient', patient)
        if(uuid):
            print("added patient: " + uuid)
            #save_json('Patient',uuid,patient)
            uuid_cur = insertPgDict('uuids', {
                'src': src,
                'row_id': record.row_id,
                'uuid': uuid
            })
        else:
            print("no uuid for " + src + " row_id: " + str(record.row_id)) 
        pg_conn.commit()
        uuid_cur.close()
    patient_cur.close()

# post admissions to openmrs fhir encounters interface
def admissionsToEncounters(limit):
    admissions_cur = getAdmissions(limit)
    for record in admissions_cur:
        stay_array={}
        print("processing admission: " + str(record.hadm_id))
        admission_data = getAdmissionData(record)
        # each admission generates a grandparent (visit encounter)
        # a parent (admission encounter)
        # and one or more icustay encounters
        visit = {
            "resourceType":
            "Encounter",
            "status":
            "finished",
            "type": [{
                "coding": [{
#                    "display": record.admission_type
                    "code": record.visit_type_code
                }]
            }],
            "subject": {
                "id": record.patient_uuid,
            },
            "period": {
                "start": deltaDate(record.admittime, record.offset),
                "end": deltaDate(record.dischtime, record.offset)
            },
            "location": [{
                "location": {
                    "reference": "Location/" + record.admission_location_uuid,
                },
                "period": {
                    "start": deltaDate(record.admittime, record.offset),
                    "end": deltaDate(record.dischtime, record.offset)
                }
            }]
        }
        visit_uuid = postDict('fhir', 'encounter', visit)
        uuid_cur = insertPgDict('uuids', {
            'src': 'mimiciii.admissions',
            'row_id': record.row_id,
            'uuid': visit_uuid
        })
        uuid_cur.close()
        admission = {
            "resourceType":
            "Encounter",
            "status":
            "finished",
            "type": [{
                "coding": [{
                    "display": record.admission_type
                }]
            }],
            "subject": {
                "id": record.patient_uuid,
            },
            "period": {
                "start": deltaDate(record.admittime, record.offset),
                "end": deltaDate(record.dischtime, record.offset)
            },
            "location": [{
                "location": {
                    "reference": "Location/" + record.admission_location_uuid,
                },
                "period": {
                    "start": deltaDate(record.admittime, record.offset),
                    "end": deltaDate(record.dischtime, record.offset)
                }
            }],
            "partOf": {
                "reference": "Encounter/" + visit_uuid,
            }
        }
        admission_uuid = postDict('fhir', 'encounter', admission)
        #save_json('Encounter',admission_uuid,admission)
        if (admission_uuid == False):
            return(False)
        for icustay in admission_data['icustays']:
            print("processing stay: " + str(icustay.icustay_id))
            icuenc = {
                "resourceType": "Encounter",
                "status": "finished",
                "type": [{
                    "coding": [{
                         "display": record.admission_type
#                        "display": "icustay"
                    }]
                }],
                "subject": {
                    "id": record.patient_uuid,
                },
                "period": {
                    "start": deltaDate(icustay.intime, record.offset),
                    "end": deltaDate(icustay.outtime, record.offset)
                },
                "location": [{
                    "location": {
                        "reference": "Location/" + location_array[icustay.first_careunit],
                    },
                    "period": {
                        "start": deltaDate(icustay.intime, record.offset),
                        "end": deltaDate(icustay.outtime, record.offset)
                    }
                }],
                "partOf": {
                    "reference": "Encounter/" + visit_uuid,
                }
            }
            icuenc_uuid = postDict('fhir', 'encounter', icuenc)
            stay_array[icustay.icustay_id] = icuenc_uuid
        for events_source in admission_data['events']:
             for event in admission_data['events'][events_source]:
                 addObs(events_source,event,record,admission_uuid,stay_array)
        addDiagnosis(record,admission_uuid)
    admissions_cur.close()
    pg_conn.commit()

def addDiagnosis(admission,encounter_uuid):
    observation = {
        "resourceType": "Observation",
        "code": {
            "coding": [{
                "system": "http://openmrs.org",
                "code": "161602AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
             }]
        },
        "subject": {
            "id": admission.patient_uuid,
        },
        "context": {
            "reference": "Encounter/" + encounter_uuid,
        },
        "effectiveDateTime":  deltaDate(admission.admittime, admission.offset),
        "valueString": admission.diagnosis
        
    }
    observation_uuid = postDict('fhir', 'observation', observation)
    return(observation_uuid)

def addObs(obs_type,obs,admission,encounter_uuid,stay_array):
    value = False
    units = False
    date = False
    concept_uuid = False
    value_type = False
    events_tables = [
        'chartevents','cptevents','datetimeevents','labevents','inputevents_cv',
        'inputevents_mv','labevents','microbiologyevents','noteevents',
        'outputevents','procedureevents_mv','procedures_icd'
    ]
    if(obs_type in ('outputevents','procedureevents_mv')):
        value_type = 'numeric'
        value = obs.value
        units = obs.valueuom
        concept_uuid = concepts_array['test_num'][obs.itemid]
    elif(obs_type in ('inputevents_cv','inputevents_mv')):
        value_type = 'numeric'
        if(obs.amount):
            value = obs.amount
            units = obs.amountuom
        elif(obs.rate):
            value = obs.rate
            units = obs.rateuom
        elif(obs.originalamount):
            value = obs.originalamount
            units = obs.originalamountuom
        elif(obs.originalrate):
            value = obs.originalrate
            units = obs.originalrateuom
        concept_uuid = concepts_array['test_num'][obs.itemid]
    elif(obs_type in ('chartevents','labevents')):
        if(obs.valuenum):
            value_type = 'numeric'
            value = obs.valuenum
            concept_uuid = concepts_array['test_num'][obs.itemid]
            if(obs.valueuom):
                units = obs.valueuom
        elif(obs.value):
            value_type = 'text'
            value = obs.value
            concept_uuid = concepts_array['test_text'][obs.itemid]
    elif(obs_type == 'noteevents'):
        value_type = 'text'
        concept_uuid = concepts_array['category'][obs.category]
        value = obs.text

    try:
        if(obs.charttime):
            date = obs.charttime
    except Exception:
        pass
    try:
        if(obs.starttime):
            date = obs.starttime
    except Exception:
        pass
    if(not date):
        try:
            if(obs.chartdate):
                date = obs.chartdate
        except Exception:
            pass
    try:
        if(obs.icustay_id):
            encounter_uuid = stay_array[obs.icustay_id]
    except Exception:
        pass

  

    if(concept_uuid and value and value_type):
        observation = {
            "resourceType": "Observation",
            "code": {
                "coding": [{
                    "system": "http://openmrs.org",
                    "code": concept_uuid
                 }]
            },
            "subject": {
                "id": admission.patient_uuid,
            },
            "context": {
                "reference": "Encounter/" + encounter_uuid,
            }
        }
        if(date):
            observation["effectiveDateTime"] =  deltaDate(date, admission.offset);
            observation["issued"] =  deltaDate(date, admission.offset);
        if(value_type == 'numeric'):
            observation["valueQuantity"] =  {
               "value": str(value),
               "unit": units ,
               "system": "http://unitsofmeasure.org",
            }
        elif(value_type == 'text'):
            observation["valueString"] = value
        observation_uuid = postDict('fhir', 'observation', observation)
        return(observation_uuid)
    else:
        print('skipping:')
        print([obs_type,concept_uuid,value_type,value,units,date,obs.row_id])
        return(None)


# post locations to openmrs fhir interface
def locationsToLocations():
    src = 'locations'
    locations_cur = getSrc(src, None)
    for record in locations_cur:
        uuid = postDict(
            'fhir',
            'location',
            {
                "resourceType": "Location",
                #        "id": record.uuid,
                "name": record.location,
                "description": record.location,
                "status": "active"
            })
        uuid_cur = insertPgDict('uuids', {
            'src': src,
            'row_id': record.row_id,
            'uuid': uuid
        })
        uuid_cur.close()
    locations_cur.close()
    pg_conn.commit()

# post encounter types to openmrs rest interface
def postEncounterTypes():
    src = 'encountertypes'
    et_cur = getSrc(src, None)
    for record in et_cur:
        uuid=postDict(
            'rest',
            'encountertype',
            {
                "name": record.encountertype,
                "description": record.encountertype,
            })
        uuid_cur = insertPgDict('uuids', {
            'src': src,
            'row_id': record.row_id,
            'uuid': uuid
        })
        uuid_cur.close()
    et_cur.close()
    pg_conn.commit()

# post visit types to openmrs rest interface
def postVisitTypes():
    src = 'visittypes'
    et_cur = getSrc(src, None)
    for record in et_cur:
        uuid=postDict(
            'rest',
            'visittype',
            {
                "name": record.visittype,
                "description": record.visittype,
            })
        uuid_cur = insertPgDict('uuids', {
            'src': src,
            'row_id': record.row_id,
            'uuid': uuid
        })
        uuid_cur.close()
    et_cur.close()
    pg_conn.commit()

# insert concepts into openmrs concepts and related tables
def conceptsToConcepts():
    src = 'concepts'
    #change the auto_increment value so we don't step on built-in concepts
    setIncrementer('concept','166000')
    setIncrementer('concept_name','166000')
    setIncrementer('concept_description','166000')
    concept_cur = getSrc(src, None)
    for record in concept_cur:
        date = time.strftime('%Y-%m-%d %H:%M:%S')
        concept_uuid = str(uuid.uuid4())
        concept = {
                "class_id": record.concept_class_id,
                "datatype_id": record.concept_datatype_id,
                "date_created": date,
                "creator": "1",
                "uuid": concept_uuid
        }
        concept_id = None
        concept_id = insertDict('concept',concept)
        concept_name_1 = {
                "concept_id": concept_id,
                "name": record.shortname,
                "date_created": date,
                "creator": "1",
                "locale": "en",
                "locale_preferred": "0",
                "concept_name_type": "SHORT",
                "uuid": str(uuid.uuid4())
            }
        insertDict('concept_name',concept_name_1)
        concept_name_2 = {
                "concept_id": concept_id,
                "name": record.longname,
                "date_created": date,
                "creator": "1",
                "locale": "en",
                "locale_preferred": "1",
                "concept_name_type": "FULLY_SPECIFIED",
                "uuid": str(uuid.uuid4())
            }
        insertDict('concept_name',concept_name_2)
        if(record.description):
            description = record.description
        else:
            description = record.shortname
        concept_description =  {
                "concept_id": concept_id,
                "date_created": date,
                "date_changed": date,
                "locale": "en",
                "creator": "1",
                "changed_by": "1",
                "description": description,
                "uuid": str(uuid.uuid4())
            }
        insertDict('concept_description',concept_description)
        if record.avg_val:
            numeric={
                    "concept_id": concept_id,
                    "precise": "1",
                    "hi_absolute":record.max_val,
                    "low_absolute":record.min_val
                }
            if record.units:
                numeric["units"]=record.units
            insertDict('concept_numeric',numeric)
        elif record.concept_type == 'test_num':
            numeric={
                    "concept_id": concept_id,
                    "precise": "1",
                }
            insertDict('concept_numeric',numeric)
        uuid_cur = insertPgDict('uuids', {
            'src': src,
            'row_id': record.row_id,
            'uuid': concept_uuid
        })
        uuid_cur.close()
        update_cur = updatePgDict('concepts', {
            'openmrs_id': concept_id,
        },{
            'row_id': record.row_id,
        })
        update_cur.close()
    concept_cur.close()
    setIncrementer('concept','3')
    setIncrementer('concept_name','21')
    setIncrementer('concept_description','1')
    pg_conn.commit()

# link enumerated concepts to their parent concepts
def genConceptMap():
    concept_cur = getConceptMap()
    for record in concept_cur:
        date = time.strftime('%Y-%m-%d %H:%M:%S')
        concept_uuid = str(uuid.uuid4())
        concept_answer = {
                "concept_id": record.parent_id,
                "answer_concept": record.child_id,
                "date_created": date,
                "creator": "1",
                "uuid": concept_uuid
        }
        insertDict('concept_answer',concept_answer);
    concept_cur.close()
    pg_conn.commit()

#TASKS
#
#initialize openmrs database (run before initial website load on fresh install)
def initDb():
    print("initializing database")
    loadPgsqlFile('../mimic/sql/add_tables.sql')

def initConcepts():
    print("import concepts")
    conceptsToConcepts()
    print("link mapped concepts")
    genConceptMap()

#rest based record creation
def initRestResources():
    locationsToLocations()
    postEncounterTypes()
    postVisitTypes()

#fhir
def initCaregivers():
    caregiversToPractitioners(None)

#fhir
def initPatients():
    getUuids()
    try:
        num = int(eval(sys.argv[2]))
    except:
        num = 1
    for x in range(0, num):
        patientsToPatients('1')
        admissionsToEncounters(None)

#fhir
def initPatient():
    patientsToPatients('1')

#fhir
def initAdmit():
    getUuids()
    admissionsToEncounters(None)

#fhir
def getUuids():
    global location_array
    global caregiver_array
    global concepts_array
    location_array = getLocations()
    caregiver_array = getCaregivers()
    concepts_array = getConcepts()
#
#MAIN
#
#connect to mimic dataset postgres
if (len(sys.argv) > 1):
    read_config()
    try:
        pg_conn = psycopg2.connect(
            dbname='mimic', user=config['PGSQL_USER'], password=config['PGSQL_PASS'])
    except Exception as e:
        print("unable to connect to the postgres databases")
        print(e)
        exit()

#connect to openmrs mysql
    try:
        mysql_conn = pymysql.connect(
            user=config['MYSQL_USER'], passwd=config['MYSQL_PASS'], db=config['SISTER'])
    except:
        print("unable to connect to the mysql database")
        exit()

    #run function from cli
    a = eval(sys.argv[1])
    a()
    pg_conn.commit()
    mysql_conn.commit()
    mysql_conn.close()
else:
    tmp = globals().copy()
    print("available arguments:")
    [print(k) for k, v in tmp.items() if k.startswith('init')]
