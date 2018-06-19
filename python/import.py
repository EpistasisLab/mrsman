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
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree

try:
    pg_conn=psycopg2.connect("dbname='mimic' user='postgres' password='postgres'")
except:
    print("unable to connect to the postgres databases")
    exit()

try:
    mysql_conn = pymysql.connect(host='127.0.0.1', user='root', passwd='password', db='ann')
    mysql_cur = mysql_conn.cursor()
except:
    print("unable to connect to the mysql database")
    exit()


def insertDict(table,Dict):
    placeholder = ", ".join(["%s"] * len(Dict))
    stmt = "insert into `{table}` ({columns}) values ({values});".format(table=table, columns=",".join(Dict.keys()), values=placeholder)
    try:
        mysql_cur.execute(stmt, list(Dict.values()))
        rowid = mysql_cur.lastrowid
        return rowid
    except Exception as e:
        print("can't insert into  "+table)
        print(e)
        exit()

def insertPgDict(table,Dict):
    pg_cur = pg_conn.cursor()
    placeholder = ", ".join(["%s"] * len(Dict))
    stmt = "insert into {table} ({columns}) values ({values});".format(table=table, columns=",".join(Dict.keys()), values=placeholder)
    try:
        pg_cur.execute(stmt, list(Dict.values()))
        return pg_cur
    except Exception as e:
        print("can't insert into  "+table)
        print(e)
        exit()





def putDict(endpoint,table,Dict):
    if(endpoint == 'fhir'):
        uri = "http://localhost:8080/openmrs/ws/fhir/" + table.capitalize() + "/" + Dict['id']
    else:
        uri = "http://localhost:8080/openmrs/ws/rest/v1/" + table + "/" + Dict['id']
    headers = {'Content-Type': 'application/json'}
    json_string = json.dumps(Dict,separators=(',', ':'))
    r = requests.put(uri, data=json_string,auth=HTTPBasicAuth('admin', 'Admin123'),headers=headers)
    print(r.text)

def postDict(endpoint,table,Dict):
    if(endpoint == 'fhir'):
        uri = "http://localhost:8080/openmrs/ws/fhir/" + table.capitalize()
    else:
        uri = "http://localhost:8080/openmrs/ws/rest/v1/" + table
    r = requests.post(uri, json=Dict,auth=HTTPBasicAuth('admin', 'Admin123'))
    return(r.headers['Location'].split('/').pop())


def getPatient(patient_uuid):
    admissions_cur = getAdmissions(patient_uuid)
    for record in admissions_cur:
       print(record)
    uri = "http://localhost:8080/openmrs/ws/fhir/Patient/" + patient_uuid + "/$everything"
    data = '<Parameters xmlns="http://hl7.org/fhir"/>'
    headers = {'Content-Type': 'text/xml'} # set what your server accepts
    r = requests.post(uri, data=data,headers=headers,auth=HTTPBasicAuth('admin', 'Admin123'))
    root = xml.etree.ElementTree.fromstring(r.content)
    for entry in root.findall('{http://hl7.org/fhir}entry'):
      for resource in entry.findall('{http://hl7.org/fhir}resource'):
        for encounter in resource.findall('{http://hl7.org/fhir}Encounter'):
           #print(encounter)
           for row in encounter:
             print(row)




#load records from table plus uuid
def getUuidSrc(table):
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    stmt = "select uuid,"+table+".* from "+table+" left join uuids on "+table+".row_id = uuids.row_id where uuids.src = '"+table+"'" 
    stmt+= " limit 10"
    try:
        pg_cur.execute(stmt)
        return pg_cur
    except Exception as e:
        print("can't SELECT from "+table)
        print(e)
        exit()

#load records from table plus uuid
def getSrc(table):
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    stmt = "select * from "+table 
    stmt+= " limit 10"
    try:
        pg_cur.execute(stmt)
        return pg_cur
    except Exception as e:
        print("can't SELECT from "+table)
        print(e)
        exit()



def getAdmissions(patient_uuid):
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    stmt = "select admission_uuid,patient_uuid,admittime,dischtime,admission_type,admission_locations.uuid admission_location_uuid,admission_location,discharge_locations.uuid discharge_location_uuid,discharge_location,edregtime,edouttime from (select uuid admission_uuid,admissions.* from admissions left join uuids on admissions.row_id = uuids.row_id where uuids.src = 'admissions') a left join (select uuid patient_uuid,patients.* from patients left join uuids on patients.row_id = uuids.row_id where uuids.src = 'patients') p  on a.subject_id = p.subject_id left join locations admission_locations on a.admission_location = admission_locations.location left join locations discharge_locations on a.discharge_location = discharge_locations.location"
    if(patient_uuid):
        stmt+=" where patient_uuid = '" +patient_uuid+ "'"
    stmt+=" limit 10"
    try:
        pg_cur.execute(stmt)
        return pg_cur
    except Exception as e:
        print("can't SELECT from "+table)
        print(e)
        exit()

def visittypesToVisittypes():
    vt_cur = getSrc('visittypes')
    for record in vt_cur:
      postDict('rest','visittype',{
        "name": record.visittype
      })


def caregiversToPractitioners():
    concept_cur = getSrc('caregivers')
    for record in concept_cur:
      birthdate = randomDate("1900-01-01", "2000-01-01")
      gender = random.choice(['male','female'])
      uuid=postDict('fhir','practitioner',{
        "resourceType": "Practitioner",
        "name": {
            "family": names.get_last_name(),
            "given": [
                names.get_first_name(gender=gender)
            ],
            "suffix": [
                record.label
            ]
        },
        "gender": gender,
        "birthDate": birthdate,
        "active": True
      })
      uuid_cur = insertPgDict('uuids',{'src':'caregivers','row_id':record.row_id,'uuid':uuid})
      uuid_cur.close()
    concept_cur.close()

def patientsToPatients():
    patient_cur = getSrc('patients')
    for record in patient_cur:
      birthDate=str(record.dob.strftime('%Y-%m-%d'))
      gender = {"M":"male","F":"female"}[record.gender]
      deceasedBoolean = {1:True,0:False}[record.expire_flag]
      OpenMRSID=str(record.row_id) + '-' + str(generate(str(record.row_id)))
      uuid=postDict('fhir','patient',{
      "resourceType": "Patient",
    #  "id": record.uuid,
      "identifier": [

        {
        "use": "usual",
        "system": "OpenMRS Identification Number",
        "value": OpenMRSID
        }
      ],
      "name": [
        {
          "use": "usual",
          "family": names.get_last_name(),
          "given": [
            names.get_first_name(gender=gender)
          ]
        }
      ],
      "gender": gender,
      "birthDate": birthDate,
      "deceasedBoolean": deceasedBoolean,
      "active": True
      })
      uuid_cur = insertPgDict('uuids',{'src':'patients','row_id':record.row_id,'uuid':uuid})
      uuid_cur.close()
    patient_cur.close()

def admissionsToEncounters(patient_uuid):
    admissions_cur = getAdmissions(patient_uuid)
    for record in admissions_cur:
      start=str(record.admittime.isoformat())
      end=str(record.dischtime.isoformat())
      print(record);
      postDict('fhir','encounter',{
#      putDict('fhir','encounter',{
#      postDict('rest','visit',{
#  "patient": record.patient_uuid,
#  "visitType": 	"7019e12e-0301-4a70-a2c4-5c833d838dfb",
#  "startDatetime": start,
#  "location": record.admission_location_uuid,
#  "indication":  record.admission_type,
#  "stopDatetime": end,
#  "attributes": [
#    {
#      "attributeType": "uuid",
#      "value": record.admission_uuid
#    }
#  ]
#        "id": record.admission_uuid,
        "resourceType": "Encounter",
        "status": "finished",
        "uuid": record.admission_uuid,
        "type": [{
            "coding": [{
                #"name": record.admission_type
                "code": "1"
                #"uuid": "34bd9255-a9d0-4e05-818b-8a462fb23d0e"
            }]
        }],
        "subject": {
            "id": record.patient_uuid,
        },
        "period": {
            "start": start,
            "end": end
        },
        "location": [{
           "location": {
#                "reference": "Location/43f5dfac-54b7-46dd-a178-89fde1322ba8",
                "id": "43f5dfac-54b7-46dd-a178-89fde1322ba8",
            },
            "period": {
                "start": start,
                "end": end
            }
        }]

    #    "id": record.admission_uuid,
    #    "resourceType": "Encounter",
    #    "type": [{
    #        "coding": [{
    #            "display": record.admission_type
    #        }]
    #    }],
    #    "subject": {
    #        "id": record.patient_uuid,
    #    },
    #    "period": {
    #        "start": start,
    #        "end": end
    #    },
    #    "location": [{
    #        "location": {
    #            "reference": "Location/" + record.admission_location_uuid,
    #            "display": record.admission_location
    #        },
    #        "period": {
    #            "start": start,
    #            "end": end
    #        }
    #    }]
      #}
      #print(encounter)
      })


def locationsToLocations():
    admissions_cur = getSrc('locations')
    for record in admissions_cur:
      putDict('fhir','location',{
        "resourceType": "Location",
        "id": record.uuid,
        "name": record.location,
        "description": record.location,
        "status": "active"
      });


def diagnosesToConcepts():
    concept_cur = getSrc('diagnoses')
    for record in concept_cur:
        if record.diagnosis == '' or record.diagnosis is None:
          description =  '[NO TEXT]'
        else:
          description = record.diagnosis
        date = time.strftime('%Y-%m-%d %H:%M:%S')
        concept_id = insertDict('concept',{
          "datatype_id": "4",
          "date_created": date,
          "class_id": "4",
          "creator": "1",
          "uuid": record.uuid 
        })
        insertDict('concept_name',{
          "concept_id": concept_id,
          "name": description,
          "date_created": date,
          "creator": "1",
          "locale": "en",
          "locale_preferred": "0",
          "concept_name_type": "SHORT",
          "uuid":  str(uuid.uuid4())
        })
        insertDict('concept_description',{
            "concept_id": concept_id,
            "date_created": date,
            "date_changed": date,
            "locale": "en",
            "creator": "1",
            "changed_by": "1",
            "description": description,
            "uuid":  str(uuid.uuid4())
        })
    concept_cur.close()

def icd9ToConcepts():
    concept_cur = getUuidSrc('d_icd_diagnoses')
    for record in concept_cur:
        short_name = record.short_title
        long_name = record.long_title
        description = record.icd9_code
        date = time.strftime('%Y-%m-%d %H:%M:%S')
        concept_id = insertDict('concept',{
          "datatype_id": "4",
          "date_created": date,
          "class_id": "4",
          "creator": "1",
          "uuid": record.uuid 
        })
        insertDict('concept_description',{
          "concept_id": concept_id,
          "date_created": date,
          "date_changed": date,
          "locale": "en",
          "creator": "1",
          "changed_by": "1",
          "description": description,
          "uuid":  str(uuid.uuid4())
        })
        insertDict('concept_name',{
          "concept_id": concept_id,
          "name": short_name,
          "date_created": date,
          "creator": "1",
          "locale": "en",
          "locale_preferred": "0",
          "concept_name_type": "SHORT",
          "uuid":  str(uuid.uuid4())
        })
        insertDict('concept_name',{
          "concept_id": concept_id,
          "name": long_name,
          "date_created": date,
          "creator": "1",
          "locale": "en",
          "locale_preferred": "1",
          "concept_name_type": "FULLY_SPECIFIED",
          "uuid":  str(uuid.uuid4())
        })
    concept_cur.close()

#    except Exception as e:



def randomDate(start, end):
    date_format = '%Y-%m-%d'
    prop=random.random()
    stime = time.mktime(time.strptime(start, date_format))
    etime = time.mktime(time.strptime(end, date_format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(date_format, time.localtime(ptime)) + 'T00:00:00'

    

#
#    except Exception as e:
#        print("Uh oh, can't connect. Invalid dbname, user or password?")
#        print(e)

#icd9ToConcepts()
#locationsToLocations()
#caregiversToPractitioners()
patientsToPatients()
#visittypesToVisittypes()
#admissionsToEncounters(None)
#admissionsToEncounters()
#admissionsToEncounters('e9a96194-58aa-4c0e-a95e-d890a24e92db')
#getPatient('e9a96194-58aa-4c0e-a95e-d890a24e92db')
pg_conn.commit()
mysql_conn.commit()
mysql_conn.close()
