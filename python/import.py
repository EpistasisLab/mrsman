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
from datetime import date
from dateutil.relativedelta import relativedelta
debug = False


# UTILITY
#
# choose a random date from a range
def randomDate(start, end):
    date_format = '%Y-%m-%d'
    prop = random.random()
    stime = time.mktime(time.strptime(start, date_format))
    etime = time.mktime(time.strptime(end, date_format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(date_format, time.localtime(ptime)) + 'T00:00:00'


#shift dates back 200 years
def shiftDate(src_date):
    return str((src_date + relativedelta(years=-200)).isoformat())


#shift dates by offset number of hours
def deltaDate(src_date, offset):
    return str((src_date + relativedelta(days=-offset)).isoformat())


#
# DATA I/O
#
#load not-yet-imported mimic records
def getSrc(table, limit):
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
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
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
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
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
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

#load all locations into an array for easy searching
def getLocations():
    cur = getSrcUuid('locations', None)
    locations = {}
    for location in cur:
        locations[location.location] = location.uuid
    return (locations)

#load all caregivers into an array for easy searching
def getCaregivers():
    cur = getSrcUuid('caregivers', None)
    caregivers = {}
    for caregiver in cur:
        caregivers[caregiver.cgid] = caregiver.uuid
    return(caregivers)

#load all caregivers into an array for easy searching
def getdLabItems():
    cur = getSrcUuid('d_labitems', None)
    labitems = {}
    for labitem in cur:
        labitems[labitem.itemid] = labitem.uuid
    return(labitems) 



#load mimic records with filter
def getSrcFilter(table, Dict):
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
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
    stmt = "insert into `{table}` ({columns}) values ({values});".format(
        table=table, columns=",".join(Dict.keys()), values=placeholder)
    try:
        mysql_cur.execute(stmt, list(Dict.values()))
        rowid = mysql_cur.lastrowid
        return rowid
    except Exception as e:
        print("can't insert into  " + table)
        print(e)
        exit()


#create record in mimic database
def insertPgDict(table, Dict):
    pg_cur = pg_conn.cursor()
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
    pg_cur = pg_conn.cursor()
    try:
        pg_cur.execute(open(filename, "r").read())
        return pg_cur
    except Exception as e:
        print("can't load file")
        print(e)
        exit()


#post a json encoded record to the fhir/rest interface
def postDict(endpoint, table, Dict):
    if (endpoint == 'fhir'):
        uri = "http://localhost:8080/openmrs/ws/fhir/" + table.capitalize()
    else:
        uri = "http://localhost:8080/openmrs/ws/rest/v1/" + table
    r = requests.post(uri, json=Dict, auth=HTTPBasicAuth('admin', 'Admin123'))
    if ("Location" in r.headers):
        return (r.headers['Location'].split('/').pop())
    else:
        print("Unexpected response:")
        print(r.text)
        return(False)

#post a json encoded record to the fhir/rest interface
def putDict(endpoint, table, Dict):
    new_uuid = str(uuid.uuid4())
    Dict['id'] = new_uuid;
    if (endpoint == 'fhir'):
        uri = "http://localhost:8080/openmrs/ws/fhir/" + table.capitalize() + "/" + new_uuid
    else:
        uri = "http://localhost:8080/openmrs/ws/rest/v1/" + table
    r = requests.put(uri, json=Dict, auth=HTTPBasicAuth('admin', 'Admin123'))
    if ("Location" in r.headers):
        return (r.headers['Location'].split('/').pop())
    else:
        print("Unexpected response:")
        print(r.text)
        return(False)

#load not-yet-imported admissions records for imported patients
def getAdmissions(limit):
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    stmt = "select hadm_id,a.row_id,visittype_uuids.uuid visit_type_uuid,discharge_location_uuids.uuid discharge_location_uuid,admission_location_uuids.uuid admission_location_uuid,patient_uuid,admittime,dischtime,admission_type,visittypes.row_id visit_type_code,admission_location,discharge_location,edregtime,edouttime,deltadate.offset from admissions a left join (select uuid patient_uuid,patients.* from patients left join uuids on patients.row_id = uuids.row_id where uuids.src = 'patients') p  on a.subject_id = p.subject_id left join locations admission_locations on a.admission_location = admission_locations.location left join locations discharge_locations on a.discharge_location = discharge_locations.location left join uuids admission_location_uuids on admission_locations.row_id = admission_location_uuids.row_id  and admission_location_uuids.src = 'locations' left join uuids discharge_location_uuids on discharge_locations.row_id = discharge_location_uuids.row_id  and discharge_location_uuids.src = 'locations' left join visittypes on a.admission_type = visittypes.visittype left join uuids visittype_uuids on visittype_uuids.row_id = visittypes.row_id and visittype_uuids.src = 'visittypes' left join deltadate on deltadate.subject_id = p.subject_id where patient_uuid is not null and a.row_id not in (select row_id from uuids where src = 'admissions')"
    if (limit):
        stmt += " limit " + limit
    try:
        pg_cur.execute(stmt)
        return pg_cur
    except Exception as e:
        print("can't load admissions")
        print(e)
        exit()


def getAdmissionData(admission):
    tables = [
        'transfers', 'icustays', 'callout', 'services', 'labevents',
        'chartevents', 'noteevents'
    ]
    admission_data = {}
    for table in tables:
        admission_data[table] = []
        cur = getSrcFilter(table, {'hadm_id': admission.hadm_id})
        for record in cur:
            admission_data[table].append(record)
    return (admission_data)



def addNote(admission,note,parent_uuid):
    return(True)



def addLab(admission,lab,encounter_uuid):
    concept_uuid = labitems_array[lab.itemid]
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
      print(observation)
      print(observation_uuid)
      return(observation_uuid)
    else:
      return False


#
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
    src = 'caregivers'
    concept_cur = getSrc(src, limit)
    for record in concept_cur:
        birthdate = randomDate("1900-01-01", "2000-01-01")
        gender = random.choice(['male', 'female'])
        uuid = postDict(
            'fhir',
            'practitioner', {
                "resourceType": "Practitioner",
                "name": {
                    "family": names.get_last_name(),
                    "given": [names.get_first_name(gender=gender)],
                    "suffix": [record.label]
                },
                "gender": gender,
                "birthDate": birthdate,
                "active": True
            })
        uuid_cur = insertPgDict('uuids', {
            'src': src,
            'row_id': record.row_id,
            'uuid': uuid
        })
        uuid_cur.close()
    concept_cur.close()
    pg_conn.commit()


# post patients to openmrs fhir interface
def patientsToPatients(limit):
    src = 'patients'
    patient_cur = getSrcDelta(src, limit)
    for record in patient_cur:
        gender = {"M": "male", "F": "female"}[record.gender]
        deceasedBoolean = {1: True, 0: False}[record.expire_flag]
        OpenMRSID = str(record.subject_id) + '-' + str(
            generate(str(record.subject_id)))
        patient = {
            "resourceType":
            "Patient",
            "identifier": [{
                "use": "usual",
                "system": "OpenMRS Identification Number",
                "value": OpenMRSID
            }],
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
        #      if(record.dod):
        #        patient["deceasedDateTime"]=shiftDate(record.dod)
        uuid = postDict('fhir', 'patient', patient)
        uuid_cur = insertPgDict('uuids', {
            'src': src,
            'row_id': record.row_id,
            'uuid': uuid
        })
        print("added patient: " + uuid)
        print(patient)
        uuid_cur.close()
    patient_cur.close()
    pg_conn.commit()


# post admissions to openmrs fhir encounters interface
def admissionsToEncounters(limit):
    admissions_cur = getAdmissions(limit)
    for record in admissions_cur:
        stay_array={}
        print("processing admission: " + str(record.hadm_id))
        admission_data = getAdmissionData(record)
        grandparent = {
            "resourceType":
            "Encounter",
            "status":
            "finished",
            "type": [{
                "coding": [{
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
        #each admission generates a grandparent encounter and a parent encounter
        grandparent_uuid = postDict('fhir', 'encounter', grandparent)
        uuid_cur = insertPgDict('uuids', {
            'src': 'admissions',
            'row_id': record.row_id,
            'uuid': grandparent_uuid
        })
        uuid_cur.close()

        parent = {
            "resourceType":
            "Encounter",
            "status":
            "finished",
            "type": [{
                "coding": [{
                    "display": "admit"
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
                "reference": "Encounter/" + grandparent_uuid,
            }
        }
        parent_uuid = postDict('fhir', 'encounter', parent)
        if (parent_uuid == False):
            return(False)
        for note in admission_data['noteevents']:
            addNote(record,note,parent_uuid)
        for lab in admission_data['labevents']:
            addLab(record,lab,parent_uuid)
        for icustay in admission_data['icustays']:
            print("processing stay: " + str(icustay.icustay_id))
            child = {
                "resourceType": "Encounter",
                "status": "finished",
                "type": [{
                    "coding": [{
                        "display": "icustay"
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
                    "reference": "Encounter/" + grandparent_uuid,
                }
            }
            child_uuid = postDict('fhir', 'encounter', child)
            stay_array[icustay.icustay_id] = child_uuid
         
    admissions_cur.close()
    pg_conn.commit()


# post locations to openmrs fhir interface
#def careUnitsToLocations():
#    for name in careunits:
#        uuid = postDict(
#            'fhir', 'location', {
#                "resourceType": "Location",
#                "name": name,
#                "description": careunits[name],
#                "status": "active"
#            })


#def servicesToLocations():
#    for name in services:
#        uuid = postDict(
#            'fhir', 'location', {
#                "resourceType": "Location",
#                "name": name,
#                "description": services[name],
#                "status": "active"
#            })


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




# summarize labevents
def getlabItems():
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    stmt = "select max(valuenum) max,min(valuenum),avg(valuenum),itemid,json_agg(distinct(valueuom)) units from labevents where valuenum is not null group by itemid order by itemid;"
    items = {}
    try:
        pg_cur.execute(stmt)
        for item in pg_cur:
          units = json.dumps(item.units)
          item_units = None
          for unit in units:
            if unit:
              item_units = unit
           
          print(units)
          items[item.itemid] = {
            'units':item_units,
            'max':item.max,
            'min':item.min
          }
        pg_cur.close()
        return(items)
    except Exception as e:
        print("can't select from labevents")
        print(e)
        exit()


# insert unique diagnoses into openmrs concept table
def dlabitemsToConcepts():
    src = 'd_labitems'
    items = getlabItems()
    concept_cur = getSrc(src, None)
    for record in concept_cur:
        if record.itemid in items.keys():
            item = items[record.itemid]
            if record.label == '' or record.label is None:
                description = '[NO TEXT]'
            else:
                description = record.label
            date = time.strftime('%Y-%m-%d %H:%M:%S')
            concept_uuid = str(uuid.uuid4())
            concept_id = insertDict(
                'concept', {
                    "datatype_id": "1",
                    "date_created": date,
                    "class_id": "1",
                    "creator": "1",
                    "uuid": concept_uuid
                })
            insertDict(
                'concept_name', {
                    "concept_id": concept_id,
                    "name": description,
                    "date_created": date,
                    "creator": "1",
                    "locale": "en",
                    "locale_preferred": "1",
                    "concept_name_type": "SHORT",
                    "uuid": str(uuid.uuid4())
                })
            insertDict(
                'concept_numeric', {
                    "concept_id": concept_id,
                    "precise": "1",
                    "hi_absolute":item['max'],
                    "low_absolute":item['min'],
#                    "units":item['units'],
                })
            uuid_cur = insertPgDict('uuids', {
                'src': src,
                'row_id': record.row_id,
                'uuid': concept_uuid
            })
            uuid_cur.close()
    concept_cur.close()
    pg_conn.commit()


# insert unique diagnoses into openmrs concept table
def ditemsToConcepts():
    src = 'd_items'
    concept_cur = getSrc(src, None)
    for record in concept_cur:
        if record.label == '' or record.label is None:
            description = '[NO TEXT]'
        else:
            description = record.label
        date = time.strftime('%Y-%m-%d %H:%M:%S')
        concept_uuid = str(uuid.uuid4())
        concept_id = insertDict(
            'concept', {
                "datatype_id": "4",
                "date_created": date,
                "class_id": "4",
                "creator": "1",
                "uuid": concept_uuid
            })
        insertDict(
            'concept_name', {
                "concept_id": concept_id,
                "name": description,
                "date_created": date,
                "creator": "1",
                "locale": "en",
                "locale_preferred": "0",
                "concept_name_type": "SHORT",
                "uuid": str(uuid.uuid4())
            })
        insertDict(
            'concept_description', {
                "concept_id": concept_id,
                "date_created": date,
                "date_changed": date,
                "locale": "en",
                "creator": "1",
                "changed_by": "1",
                "description": description,
                "uuid": str(uuid.uuid4())
            })
        uuid_cur = insertPgDict('uuids', {
            'src': src,
            'row_id': record.row_id,
            'uuid': concept_uuid
        })
        uuid_cur.close()
    concept_cur.close()
    pg_conn.commit()


# insert unique diagnoses into openmrs concept table
def diagnosesToConcepts():
    src = 'diagnoses'
    concept_cur = getSrc(src, None)
    for record in concept_cur:
        if record.diagnosis == '' or record.diagnosis is None:
            description = '[NO TEXT]'
        else:
            description = record.diagnosis
        date = time.strftime('%Y-%m-%d %H:%M:%S')
        concept_uuid = str(uuid.uuid4())
        concept_id = insertDict(
            'concept', {
                "datatype_id": "4",
                "date_created": date,
                "class_id": "4",
                "creator": "1",
                "uuid": concept_uuid
            })
        insertDict(
            'concept_name', {
                "concept_id": concept_id,
                "name": description,
                "date_created": date,
                "creator": "1",
                "locale": "en",
                "locale_preferred": "0",
                "concept_name_type": "SHORT",
                "uuid": str(uuid.uuid4())
            })
        insertDict(
            'concept_description', {
                "concept_id": concept_id,
                "date_created": date,
                "date_changed": date,
                "locale": "en",
                "creator": "1",
                "changed_by": "1",
                "description": description,
                "uuid": str(uuid.uuid4())
            })
        uuid_cur = insertPgDict('uuids', {
            'src': src,
            'row_id': record.row_id,
            'uuid': concept_uuid
        })
        uuid_cur.close()
    concept_cur.close()
    pg_conn.commit()


# insert icd9 diagnosis codes into openmrs concept table
def icd9ToConcepts():
    src = 'd_icd_diagnoses'
    concept_cur = getSrc(src, None)
    for record in concept_cur:
        short_name = record.short_title
        long_name = record.long_title
        description = record.icd9_code
        date = time.strftime('%Y-%m-%d %H:%M:%S')
        concept_uuid = str(uuid.uuid4())
        concept_id = insertDict(
            'concept', {
                "datatype_id": "4",
                "date_created": date,
                "class_id": "4",
                "creator": "1",
                "uuid": concept_uuid
            })
        insertDict(
            'concept_description', {
                "concept_id": concept_id,
                "date_created": date,
                "date_changed": date,
                "locale": "en",
                "creator": "1",
                "changed_by": "1",
                "description": description,
                "uuid": str(uuid.uuid4())
            })
        insertDict(
            'concept_name', {
                "concept_id": concept_id,
                "name": short_name,
                "date_created": date,
                "creator": "1",
                "locale": "en",
                "locale_preferred": "0",
                "concept_name_type": "SHORT",
                "uuid": str(uuid.uuid4())
            })
        insertDict(
            'concept_name', {
                "concept_id": concept_id,
                "name": long_name,
                "date_created": date,
                "creator": "1",
                "locale": "en",
                "locale_preferred": "1",
                "concept_name_type": "FULLY_SPECIFIED",
                "uuid": str(uuid.uuid4())
            })
        uuid_cur = insertPgDict('uuids', {
            'src': src,
            'row_id': record.row_id,
            'uuid': concept_uuid
        })
        uuid_cur.close()
    concept_cur.close()
    pg_conn.commit()


#
#TASKS
#
#initialize openmrs database (run before initial website load on fresh install)
def initDb():
    print("initializing database")
    loadPgsqlFile('../mimic/sql/add_tables.sql')
    print("generate concepts from d_labitems")
    dlabitemsToConcepts()
    print("generate concepts from icd9 codes")
    icd9ToConcepts()
    print("generate concepts from diagnoses")
    diagnosesToConcepts()
    print("generate concepts from ditems")
    ditemsToConcepts()
    print("generate visit types")
    visittypestoVisitTypes()


#initialize
def initLocations():
    locationsToLocations()


def initPractitioners():
    caregiversToPractitioners(None)


def initPatients():
    patientsToPatients('1')
    #admissionsToEncounters(None)


def initAdmit():
    global location_array
    global caregiver_array
    global labitems_array
    location_array = getLocations()
    caregiver_array = getCaregivers()
    labitems_array = getdLabItems()
    admissionsToEncounters(None)


#
#MAIN
#
#connect to mimic dataset postgres
if (len(sys.argv) > 1):
    try:
        pg_conn = psycopg2.connect(
            "dbname='mimic' user='postgres' password='postgres'")
    except:
        print("unable to connect to the postgres databases")
        exit()

#connect to openmrs mysql
    try:
        mysql_conn = pymysql.connect(
            host='127.0.0.1', user='root', passwd='password', db='ann')
        mysql_cur = mysql_conn.cursor()
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
