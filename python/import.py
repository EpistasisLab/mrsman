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
from datetime import date
from dateutil.relativedelta import relativedelta
debug = False
baseuri = "http://localhost:8080/openmrs/ws"
sister = 'kate'


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
            # for alternating digits starting with the rightmost, we
            # use our formula this is the same as multiplying x 2 and
            # adding digits together for values 0 to 9.  Using the
            # following formula allows us to gracefully calculate a
            # weight for non-numeric "digits" as well (from their
            # ASCII value - 48).
            weight = (2 * digit)
        else:
            # even-positioned digits just contribute their ascii
            # value minus 48
            weight = digit
        # keep a running total of weights
        sum += weight
    # avoid sum less than 10 (if characters below "0" allowed,
    # this could happen)
    sum = math.fabs(sum) + mod
    # check digit is amount needed to reach next number
    # divisible by ten. Return an integer
    checkdigit =  int((mod - (sum % mod)) % mod)
    #if(as_txt):
    #    checkdigit = valid_chars[checkdigit]
    return  valid_chars[checkdigit]


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


#shift dates by offset number of hours
def deltaDate(src_date, offset):
    return str((src_date + relativedelta(days=-offset)).isoformat())

def openPgCursor():
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    pg_cur.execute("SET search_path TO " + sister)
    return(pg_cur)


#
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



#load all note categories into an array for easy searching
def getNoteCategories():
    cur = getSrcUuid('mimic.notecategories', None)
    categories = {}
    for category in cur:
        categories[category.category] = category.uuid
    return (categories)


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

#load all caregivers into an array for easy searching
def getdLabItems():
    cur = getSrcUuid('mimiciii.d_labitems', None)
    labitems = {}
    for labitem in cur:
        labitems[labitem.itemid] = labitem.uuid
    return(labitems) 

#load all caregivers into an array for easy searching
def getConcepts():
    cur = getSrcUuid('concepts', None)
    concepts = {}
    concepts['test_num'] = {}
    concepts['test_text'] = {}
    concepts['test_enum'] = {}
    concepts['diagnosis'] = {}
    concepts['answer'] = {}
    concepts['category'] = {}
    for concept in cur:
        if concept.concept_type in ['test_num','test_text','test_enum']:
            concepts[concept.concept_type][concept.itemid] = concept.uuid
        elif concept.concept_type in ['diagnosis','answer','category']:
            concepts[concept.concept_type][concept.shortname] = concept.uuid
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
    stmt = "insert into `{table}` ({columns}) values ({values});".format(
        table=table, columns=",".join(Dict.keys()), values=placeholder)
    try:
        mysql_cur.execute(stmt, list(Dict.values()))
        rowid = mysql_cur.lastrowid
        return rowid
    except Exception as e:
        print("can't insert into  " + table)
        print(e)
        print('failed record')
        print(Dict)
        exit()

#create record in mimic database
def updatePgDict(table, Dict, Filter):
    pg_cur = openPgCursor()
    #pg_cur = pg_conn.cursor()
    placeholder = ", ".join(["%s"] * len(Dict))
    stmt = "update {table} set ({columns}) = ({values})".format(
        table=table, columns=",".join(Dict.keys()), values=placeholder)
    for col_name in Filter:
        stmt += " where " + col_name + " = '" + str(Filter[col_name]) + "'"
    try:
        pg_cur.execute(stmt, list(Dict.values()))
        return pg_cur
    except Exception as e:
        print("can't insert into  " + table)
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
        return pg_cur
    except Exception as e:
        print("can't load file")
        print(e)
        exit()

#post a json encoded record to the fhir/rest interface
def postDict(endpoint, table, Dict):
    if (endpoint == 'fhir'):
        uri = baseuri + "/fhir/" + table.capitalize()
    else:
        uri = baseuri + "/rest/v1/" + table
    r = requests.post(uri, json=Dict, auth=HTTPBasicAuth('admin', 'Admin123'))
    if debug:
        print('post:')
        print(Dict)
        print('response:')
        print(r)
    if ("Location" in r.headers):
        return (r.headers['Location'].split('/').pop())
    else:
        response = json.loads(r.text)
        if ('uuid' in response):
            uuid = response['uuid']
            return(uuid)
        else:
            print("Unexpected response:")
            print(r.text)
        return(False)



#post a json encoded record to the fhir/rest interface
def putDict(endpoint, table, Dict):
    new_uuid = str(uuid.uuid4())
    Dict['id'] = new_uuid;
    if (endpoint == 'fhir'):
        uri = baseuri + "/fhir/" + table.capitalize() + "/" + new_uuid
    else:
        uri = baseuri + "/rest/v1/" + table
    r = requests.put(uri, json=Dict, auth=HTTPBasicAuth('admin', 'Admin123'))
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
    stmt = "select hadm_id,a.row_id,visittype_uuids.uuid visit_type_uuid,discharge_location_uuids.uuid discharge_location_uuid,admission_location_uuids.uuid admission_location_uuid,patient_uuid,admittime,dischtime,admission_type,visittypes.row_id visit_type_code,admission_location,discharge_location,edregtime,edouttime,deltadate.offset from mimiciii.admissions a left join (select uuid patient_uuid,patients.* from mimiciii.patients left join uuids on mimiciii.patients.row_id = uuids.row_id where uuids.src = 'mimiciii.patients') p  on a.subject_id = p.subject_id left join locations admission_locations on a.admission_location = admission_locations.location left join locations discharge_locations on a.discharge_location = discharge_locations.location left join uuids admission_location_uuids on admission_locations.row_id = admission_location_uuids.row_id  and admission_location_uuids.src = 'locations' left join uuids discharge_location_uuids on discharge_locations.row_id = discharge_location_uuids.row_id  and discharge_location_uuids.src = 'locations' left join visittypes on a.admission_type = visittypes.visittype left join uuids visittype_uuids on visittype_uuids.row_id = visittypes.row_id and visittype_uuids.src = 'visittypes' left join deltadate on deltadate.subject_id = p.subject_id where patient_uuid is not null and a.row_id not in (select row_id from uuids where src = 'mimiciiii.admissions')"
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
        else:
            print("Caregiver not created")
            pg_conn.commit()
            exit()
    concept_cur.close()
    pg_conn.commit()


# post patients to openmrs fhir interface
def patientsToPatients(limit):
    src = 'mimiciii.patients'
    patient_cur = getSrcDelta(src, limit)
    for record in patient_cur:
        gender = {"M": "male", "F": "female"}[record.gender]
        deceasedBoolean = {1: True, 0: False}[record.expire_flag]
        OpenMRSIDnumber = str(record.subject_id) + '-' + str(
            generate(str(record.subject_id)))
        OpenMRSID = str(record.subject_id) + str(
            luhnmod30(str(record.subject_id)))
        patient = {
            "resourceType":
            "Patient",
            "identifier": [{
                "system": "OpenMRS Identification Number",
                "use": "secondary",
                "value": OpenMRSIDnumber
            },{
                "use": "usual",
                "system": "OpenMRS ID",
                "value": OpenMRSID
            }
            ],
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
                "reference": "Encounter/" + grandparent_uuid,
            }
        }
        parent_uuid = postDict('fhir', 'encounter', parent)
        if (parent_uuid == False):
            return(False)
        for lab in admission_data['labevents']:
            addLab(record,lab,parent_uuid)
        for icustay in admission_data['icustays']:
            print("processing stay: " + str(icustay.icustay_id))
            child = {
                "resourceType": "Encounter",
                "status": "finished",
                "type": [{
                    "coding": [{
#                        "display": "icustay"
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
                    "reference": "Encounter/" + grandparent_uuid,
                }
            }
            child_uuid = postDict('fhir', 'encounter', child)
            stay_array[icustay.icustay_id] = child_uuid
        for note in admission_data['noteevents']:
            addNote(record,note,parent_uuid)
        for chart in admission_data['chartevents']:
            if (chart.valuenum):
              addnumChart(record,chart,parent_uuid)
            else:
              addtxtChart(record,chart,parent_uuid)
         
    admissions_cur.close()
    pg_conn.commit()


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


# post locations to openmrs fhir interface
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

# post locations to openmrs fhir interface
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

# summarize chartevents
def getchartItems_num():
    pg_cur = openPgCursor()
    #pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    stmt = "select max(valuenum) max,min(valuenum),avg(valuenum),itemid,json_agg(distinct(valueuom)) units from chartevents where valuenum is not null group by itemid order by itemid;"
    items = {}
    try:
        pg_cur.execute(stmt)
        for item in pg_cur:
          units = None
          #pick non-empty units
          for u in item.units:
            if not units and u and not u.isspace():
              units = u
          items[item.itemid] = {
            'units':item.units[0],
            'max':item.max,
            'min':item.min
          }
        pg_cur.close()
        return(items)
    except Exception as e:
        print("can't select from chartevents")
        print(e)
        exit()


# summarize chartevents
def getchartItems_txt():
    pg_cur = openPgCursor()
    #pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    stmt = "SELECT itemid from chartevents where not value ~ '^([0-9]+[.]?[0-9]*|[.][0-9]+)$' group by itemid"
    items = {}
    try:
        pg_cur.execute(stmt)
        for item in pg_cur:
          #pick non-empty units
          items[item.itemid] = {
            'itemid':item.itemid,
          }
        pg_cur.close()
        return(items)
    except Exception as e:
        print("can't select from chartevents")
        print(e)
        exit()




# summarize labevents
def getlabItems():
    pg_cur = openPgCursor()
    #pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    stmt = "select max(valuenum) max,min(valuenum),avg(valuenum),itemid,json_agg(distinct(valueuom)) units from labevents where valuenum is not null group by itemid order by itemid;"
    items = {}
    try:
        pg_cur.execute(stmt)
        for item in pg_cur:
          units = None
          #pick non-empty units
          for u in item.units:
            if not units and u and not u.isspace():
              units = u
          items[item.itemid] = {
            'units':item.units[0],
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
def chartitemsnumToConcepts():
    src = 'mimiciiii.d_items'
    items = getchartItems_num()
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
            numeric={
                    "concept_id": concept_id,
                    "precise": "1",
                    "hi_absolute":item['max'],
                    "low_absolute":item['min'],
                }
            if item['units']:
                numeric["units"]=item['units']
            insertDict('concept_numeric',numeric)



            uuid_cur = insertPgDict('uuids', {
                'src': src,
                'row_id': record.row_id,
                'uuid': concept_uuid
            })
            uuid_cur.close()
    concept_cur.close()
    pg_conn.commit()


# insert unique diagnoses into openmrs concept table
def chartitemstxtToConcepts():
    src = 'mimiciii.d_items'
    items = getchartItems_txt()
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
                    "datatype_id": "3",
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
            uuid_cur = insertPgDict('uuids', {
                'src': src,
                'row_id': record.row_id,
                'uuid': concept_uuid
            })
            uuid_cur.close()
    concept_cur.close()
    pg_conn.commit()


# insert unique diagnoses into openmrs concept table
def dlabitemsToConcepts():
    src = 'mimiciii.d_labitems'
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
            numeric={
                    "concept_id": concept_id,
                    "precise": "1",
                    "hi_absolute":item['max'],
                    "low_absolute":item['min'],
                }
            if item['units']:
                numeric["units"]=item['units']
            insertDict('concept_numeric',numeric)
            uuid_cur = insertPgDict('uuids', {
                'src': src,
                'row_id': record.row_id,
                'uuid': concept_uuid
            })
            uuid_cur.close()
    concept_cur.close()
    pg_conn.commit()


# insert unique diagnoses into openmrs concept table
def notecategoriesToConcepts():
    src = 'notecategories'
    concept_cur = getSrc(src, None)
    for record in concept_cur:
        date = time.strftime('%Y-%m-%d %H:%M:%S')
        concept_uuid = str(uuid.uuid4())
        concept_id = insertDict(
            'concept', {
                "datatype_id": "3",
                "date_created": date,
                "class_id": "7",
                "creator": "1",
                "uuid": concept_uuid
            })
        insertDict(
            'concept_name', {
                "concept_id": concept_id,
                "name": record.category,
                "date_created": date,
                "creator": "1",
                "locale": "en",
                "locale_preferred": "1",
                "concept_name_type": "FULLY_SPECIFIED",
                "uuid": str(uuid.uuid4())
            })
        insertDict(
            'concept_name', {
                "concept_id": concept_id,
                "name": record.category,
                "date_created": date,
                "creator": "1",
                "locale": "en",
                "locale_preferred": "0",
                "concept_name_type": "SHORT",
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


# insert unique diagnoses into openmrs concept table
def ditemsToConcepts():
    src = 'mimiciii.d_items'
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


# insert concepts into openmrs concept table
def conceptsToConcepts():
    src = 'concepts'
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
        concept_description =  {
                "concept_id": concept_id,
                "date_created": date,
                "date_changed": date,
                "locale": "en",
                "creator": "1",
                "changed_by": "1",
                "description": record.shortname,
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
    pg_conn.commit()

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


# insert unique diagnoses into openmrs concept table

# insert unique diagnoses into openmrs concept table
#def diagnosesToConcepts():
#    src = 'diagnoses'
#    concept_cur = getSrc(src, None)
#    for record in concept_cur:
#        if record.diagnosis == '' or record.diagnosis is None:
#            description = '[NO TEXT]'
#        else:
#            description = record.diagnosis
#        date = time.strftime('%Y-%m-%d %H:%M:%S')
#        concept_uuid = str(uuid.uuid4())
#        concept_id = insertDict(
#            'concept', {
#                "datatype_id": "4",
#                "date_created": date,
#                "class_id": "4",
#                "creator": "1",
#                "uuid": concept_uuid
#            })
#        insertDict(
#            'concept_name', {
#                "concept_id": concept_id,
#                "name": description,
#                "date_created": date,
#                "creator": "1",
#                "locale": "en",
#                "locale_preferred": "0",
#                "concept_name_type": "SHORT",
#                "uuid": str(uuid.uuid4())
#            })
#        insertDict(
#            'concept_description', {
#                "concept_id": concept_id,
#                "date_created": date,
#                "date_changed": date,
#                "locale": "en",
#                "creator": "1",
#                "changed_by": "1",
#                "description": description,
#                "uuid": str(uuid.uuid4())
#            })
#        uuid_cur = insertPgDict('uuids', {
#            'src': src,
#            'row_id': record.row_id,
#            'uuid': concept_uuid
#        })
#        uuid_cur.close()
#    concept_cur.close()
#    pg_conn.commit()


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

#direct database record creation
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
    patientsToPatients(None)
#    initAdmit()


#fhir
def initAdmit():
    global location_array
    global caregiver_array
    global concepts_array
#    global labitems_array
#    global ditems_array
#    global notecategory_array
#    notecategory_array = getNoteCategories()
    location_array = getLocations()
    caregiver_array = getCaregivers()
#    labitems_array = getdLabItems()
#    ditems_array = getdItems()
    concepts_array = getConcepts()
    admissionsToEncounters(None)


#
#MAIN
#
#connect to mimic dataset postgres
if (len(sys.argv) > 1):
    try:
        pg_conn = psycopg2.connect(
            dbname='mimic', user='postgres', password='postgres')
    except Exception as e:
        print("unable to connect to the postgres databases")
        print(e)
        exit()

#connect to openmrs mysql
    try:
        mysql_conn = pymysql.connect(
            host='127.0.0.1', user='root', passwd='password', db=sister)
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
