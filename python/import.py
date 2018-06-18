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
 
try:
    pg_conn=psycopg2.connect("dbname='mimic' user='postgres' password='postgres'")
#    pg_cur = pg_conn.cursor(pymysql.cursors.DictCursor)
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


def postDict(table,Dict):
    uri = "http://localhost:8080/openmrs/ws/fhir/" + table.capitalize()
    r = requests.post(uri, json=Dict,auth=HTTPBasicAuth('admin', 'Admin123'))
    print(r.text)

def getSrc(table):
    #get records in table plus uuid
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    stmt = "select uuid,"+table+".* from "+table+" left join uuids on "+table+".row_id = uuids.row_id where uuids.src = '"+table+"' limit 1" 
    try:
        pg_cur.execute(stmt)
        return pg_cur
    except Exception as e:
        print("can't SELECT from "+table)
        print(e)
        exit()


def caregiversToPractitioners():
    concept_cur = getSrc('caregivers')
    for record in concept_cur:
      birthdate = randomDate("1900-01-01", "2000-01-01")
      gender = random.choice(['male','female'])
      postDict('practitioner',{
        "resourceType": "Practitioner",
        "id": record.uuid,
        "name": {
            "family": [
                 names.get_last_name()
            ],
            "given": [
                names.get_first_name(gender=gender)
            ],
            "suffix": [
                record.label
            ]
        },
        "address": [{
            "use": "home",
            "city": "city"
        }],
        "gender": gender,
        "birthDate": birthdate,
      })


def patientsToPatients():
    patient_cur = getSrc('patients')
    for record in patient_cur:
      print(record.row_id)
      OpenMRSID=str(record.row_id) + '-' + str(generate(str(record.row_id)))
      postDict('patient',{
      "resourceType": "Patient",
      "id": record.uuid,
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
          "family": [
            "Chalmers"
          ],
          "given": [
            "Peter",
            "James"
          ]
        }
      ],
      "gender": "male",
      "birthDate": "1974-12-25",
      "address": [
        {
          "city": "city",
        }
      ],
      })







def diagnosesToConcepts():
    concept_cur = getSrc('diagnoses')
    for record in concept_cur:
        if record.diagnoses == '' or record.diagnoses is None:
          description =  '[NO TEXT]'
        else:
          description = record.diagnoses
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
    concept_cur = getSrc('d_icd_diagnoses')
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
#caregiversToPractitioners()
patientsToPatients()
pg_conn.commit()
mysql_conn.commit()
mysql_conn.close()
