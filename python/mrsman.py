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
import threading
from luhn import *
from metadata import *
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree
import sys
import math
import os
import copy
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
import fhirclient.models.encounter as Encounter
import fhirclient.models.patient as Patient
from fhirclient import client
import numpy as np
import pandas as pd
debug = False
numThreads = 1
saveFiles = True
exitFlag = False
shiftDates = True
config_file = 'config.json'
create_omrs_obs = False

class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)
#THREADING
#
class mrsThread (threading.Thread):
   def __init__(self, threadID,parent):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = 'Thread' + str(threadID)
   def run(self):
      bootstrap(self)
      print ("Starting %s: %s" % (self.name, time.ctime(time.time())))
      while self.counter:
          if exitFlag:
              break
          print ("thread %s, iter %s" % (self.name, self.counter))
          if not (handleRecords(self)):
              break
          self.counter -= 1
      print ("Exiting %s: %s" % (self.name, time.ctime(time.time())))
      shutdown(self)

#
def runTask(self):
    if (numThreads > 1):
        splitTask(self)
    else:
        if(self.num):
            self.limit = self.num
        else:
            self.limit = False
        handleRecords(self)

#load num records from src using callback - split among numThreads
def splitTask(self):
    if(self.num and self.num < numThreads):
        self.nt = self.num
    else:
        self.nt = numThreads
    if(self.num):
        self.counter = math.floor(self.num/self.nt)
        self.limit = 1
    else:
        self.counter = 1
        self.limit = False
    threads = {}
    #set up threads
    for x in range(0, self.nt):
      threads[x] = mrsThread(x,self)
      #number of threads
      threads[x].nt = self.nt
      #records to fetch at once
      threads[x].limit = self.limit
      #number of iterations for each thread
      threads[x].counter = self.counter
      #this thread number
      threads[x].x = x
      #source table
      threads[x].src = self.src
      #processor for a record
      threads[x].callback = self.callback
      #
      threads[x].arg = self.arg
      if ('uuid' in dir(self)):
          threads[x].uuid = self.uuid
      if ('deltadate' in dir(self)):
          threads[x].deltadate  = self.deltadate
    #start threads
    for x in threads:
      thread = threads[x]
      thread.start()
    for x in threads:
      thread = threads[x]
      thread.join()

#fetch mimic db records
def getSrc(self):
    pg_cur = openPgCursor(self)
    limit = False
    uuid = False
    deltadate = False
    Filter = {}
    #limit the number of records to return
    if ('limit' in dir(self) and self.limit > 0):
        limit = str(self.limit)
    #return records with a uuid (1) or none (-1)
    if ('uuid' in dir(self)):
        uuid = self.uuid
    #join with deltadate table
    if ('deltadate' in dir(self)):
        deltadate = self.deltadate
    #include a subset of records based on mod x
    if ('x' in dir(self) and self.x is not False):
        Filter.update({"MOD("+self.src+".row_id,"+ str(numThreads) +")":str(self.x)})
    #filter records based on an array of key/value pairs
    if ('filter' in dir(self) and self.filter):
        Filter.update(self.filter)
    stmt = "select " + self.src + ".*"
    if (uuid >= 1):
        stmt += ",uuids.uuid"
    if (deltadate):
        stmt += ",deltadate.offset from " + self.src + " left join deltadate on deltadate.subject_id = " + self.src + ".subject_id"
    else:
        stmt += " from " + self.src
    if(uuid >= 1):
        stmt += " left join uuids on " + self.src + ".row_id = uuids.row_id and uuids.src = '" + self.src + "'"
    if(Filter):
        stmt += " where "
        fields = []
        for col_name in Filter:
            fields.append(col_name + " = '" + str(Filter[col_name]) + "'")
        stmt += ' and ' .join(fields)
        if(uuid == -1):
          stmt +=  " and row_id not in (select row_id from uuids where src = '" + self.src + "')"
    else:
        if(uuid == -1):
          stmt +=  " where row_id not in (select row_id from uuids where src = '" + self.src + "')"
        if(uuid == 1):
          stmt +=  " where uuid is not null"
        if(uuid == 2):
          stmt +=  " where uuid is not null and state = 0"
    if(limit):
        stmt += " limit " + limit
    if debug:
        print(stmt)
    try:
        pg_cur.execute("SET search_path TO mimiciii," + config['SISTER'])
        pg_cur.execute(stmt)
        return pg_cur
    except Exception as e:
        print("can't select from " + self.src)
        print(e)
        exit()

#use records in a cursor in callback, saving the uuid if it doesn't already exist
def handleRecords(self):
    cur = getSrc(self)
    success = 0
    child = copy.copy(self)
    child.x = False
    for record in cur:
        #execute the callback
        uuid=self.callback(child,record)
        if (uuid):
           print("added "+self.src+": " + uuid)
           self.last_uuid = uuid;
           if(self.uuid == -1):
               uuid_cur = insertPgDict(self,'uuids', {
                   'src': self.src,
                   'row_id': record.row_id,
                   'uuid': uuid
               })
               uuid_cur.close()
           success += 1
           self.pg_conn.commit()
        elif (self.uuid == -1):
            print("no uuid for " + child.src + " row_id: " + str(record.row_id)) 
    cur.close()
    self.pg_conn.commit()
    if(success > 0):
        return(True)
    else:
        return(False)

# UTILITY
#
#write dictionary to a file
def save_json(model_type,uuid,data):
    json_path = '/data/devel/mrsman/data/json'
    directory = json_path + '/' + model_type
    filename = directory + '/' + uuid + '.json'
    if not os.path.exists(directory):
        try:
            os.makedirs(directory) 
        except Exception as e:
            print(e)
    with open(filename, 'w') as outfile:
        json.dump(data, outfile)

#open a postgres cursor 
def openPgCursor(self):
    pg_cur = self.pg_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    return(pg_cur)


#read config file and initialize database connections
def bootstrap(self):
    global config
    global smart
    #parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(sys.argv[0])))
    parent_dir = '/data/devel/mrsman'
    with open(parent_dir + '/' + config_file) as f:
        data = json.load(f)
        config = data['global']
        config['baseuri'] = 'http://' + config['IP'] + ':' +  config['OPENMRS_PORT'] + '/openmrs/ws'
    #connect to openmrs psql
    try:
        self.pg_conn = psycopg2.connect(
            dbname='mimic', user=config['PGSQL_USER'], password=config['PGSQL_PASS'], host='localhost')
    except Exception as e:
        print("unable to connect to the postgres databases")
        print(e)
        exit()
    #connect to openmrs mysql
    try:
        self.mysql_conn = pymysql.connect(
            user=config['MYSQL_USER'], passwd=config['MYSQL_PASS'], db=config['SISTER'])
    except Exception as e:
        print("unable to connect to the mysql database")
        print(e)
        exit()
    settings = {
        'app_id': 'mrsman',
        'api_base': config['baseuri'] + '/fhir/'
    }
    smart = client.FHIRClient(settings=settings)
    smart.server.session.auth=HTTPBasicAuth(config['OPENMRS_USER'],config['OPENMRS_PASS'])
    return()


# load uuids into memory
def getUuids(self):
    global uuid_array
    bootstrap(self)
    uuid_array = {}
    index_fields = {
                   'admissions':'hadm_id',
                   'visits':'hadm_id',
                   'icustays':'icustay_id',
                   'patients':'subject_id',
                   'caregivers':'cgid',
                   'locations':'location',
                   }
    for table_name in index_fields:
        child = copy.copy(self)
        child.src = table_name
        child.index_on = index_fields[table_name]
        uuid_array[table_name] = genIndex(child)
    uuid_array['concepts'] = getConcepts(self)


# load uuids into memory
def getSubjectVisits(self):
    global subjects
    mrsman.getUuids(self)
    self.src = 'visits'
    self.callback = mrsman.addEvents
    if(self.num):
        mrsman.numThreads = 1
        self.filter = {'hadm_id':self.num}
    else:
        mrsman.numThreads = 50
    self.num = False
    mrsman.runTask(self)


# close db connections
def shutdown(self):
    self.pg_conn.commit()
    self.mysql_conn.commit()
    self.pg_conn.close()
    self.mysql_conn.close()

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
    if(shiftDates):
        return str((src_date + relativedelta(days=-offset)).isoformat())
    else:
        return str(src_date.isoformat())

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


# DATA I/O
#
#get enumerated concepts
def getConceptMap(self):
    pg_cur = openPgCursor(self)
    stmt = "select concepts.openmrs_id parent_id,cm.openmrs_id child_id from (select cetxt_map.itemid,concepts.openmrs_id from cetxt_map left join concepts on cetxt_map.value = concepts.shortname and concepts.concept_type = 'answer') cm left join concepts on cm.itemid = concepts.itemid and concepts.concept_type = 'test_enum'"
    try:
        pg_cur.execute(stmt)
        return pg_cur
    except Exception as e:
        print("can't select from " + table)
        print(e)
        exit()

#create record in openmrs database
def insertDict(self, table, Dict):
    placeholder = ", ".join(["%s"] * len(Dict))
    mysql_cur = self.mysql_conn.cursor()
    stmt = "insert into `{table}` ({columns}) values ({values});".format(
        table=table, columns=",".join(Dict.keys()), values=placeholder)
    if debug:
        print(stmt)
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
def setIncrementer(self, table, value):
    mysql_cur = self.mysql_conn.cursor()
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

#update record in mimic database
def updatePgDict(self, table, Dict, Filter):
    pg_cur = openPgCursor(self)
    placeholder = ", ".join(["%s"] * len(Dict))
    #stmt = "update {table} set ({columns}) = ROW({values})".format(
    stmt = "update {table} set {columns} = {values}".format(
        table=table, columns=",".join(Dict.keys()), values=placeholder)
    if(Filter):
        stmt += " where "
        fields = []
        for col_name in Filter:
             fields.append(col_name + " = '" + str(Filter[col_name]) + "'")
        stmt += ' and ' .join(fields)
    if debug:
        print(stmt)
    try:
        pg_cur.execute(stmt, list(Dict.values()))
        return pg_cur
    except Exception as e:
        print("can't update " + table)
        print(stmt)
        print(Dict.values())
        print(e)
        exit()


#create record in mimic database
def deletePgDict(self, table, Filter):
    pg_cur = openPgCursor(self)
    #stmt = "update {table} set ({columns}) = ROW({values})".format(
    stmt = "delete from " + table + " where " 
    fields = []
    for col_name in Filter:
        fields.append(col_name + " = '" + str(Filter[col_name]) + "'")
    stmt += ' and ' .join(fields)
    try:
        print(stmt)
        pg_cur.execute(stmt)
        return pg_cur
    except Exception as e:
        print("can't delete from " + table)
        print(stmt)
        print(e)
        exit()

#create record in postgres database
def insertPgDict(self,table, Dict):
    #pg_cur = pg_conn.cursor()
    pg_cur = openPgCursor(self)
    placeholder = ", ".join(["%s"] * len(Dict))
    stmt = "insert into {table} ({columns}) values ({values});".format(
        table=table, columns=",".join(Dict.keys()), values=placeholder)
    if debug:
        print(stmt)
        print(Dict)
    try:
        pg_cur.execute(stmt, list(Dict.values()))
        return pg_cur
    except Exception as e:
        print("can't insert into  " + table)
        print(e)
        exit()

#run sql from file in openmrs database
def loadMySQLFile(self,filename):
    mysql_cur = self.mysql_conn.cursor()
    try:
        for line in open(filename, "r"):
            mysql_cur.execute(line)
        self.mysql_conn.commit()
        return mysql_cur
    except Exception as e:
        print("can't load file")
        print(e)
        exit()

#run sql from file in mimic database
def loadPgsqlFile(self,filename):
    pg_cur = openPgCursor(self)
    #pg_cur = pg_conn.cursor()
    try:
        pg_cur.execute("SET search_path TO " + config['SISTER'])
        pg_cur.execute(open(filename, "r").read())
        self.pg_conn.commit()
        return pg_cur
    except Exception as e:
        print("can't load file")
        print(e)
        exit()

#post a json encoded record to the fhir/rest interface
def postDict(endpoint, table, Dict):
    if (endpoint == 'fhir'):
        uri = config['baseuri'] + "/fhir/" + table
    else:
        uri = config['baseuri'] + "/rest/v1/" + table.lower()
    r = requests.post(uri, json=Dict, auth=HTTPBasicAuth(config['OPENMRS_USER'], config['OPENMRS_PASS']),headers={'Connection':'close'},  stream=False)
    r.connection.close()
    if debug:
        print('post:')
        print(Dict)
        print('response:')
        print(r.headers)
    if ("Location" in r.headers):
        uuid = r.headers['Location'].split('/').pop()
        if(saveFiles):
            save_json(table,uuid,Dict)
        return (uuid)
    else:
        response = json.loads(r.text)
        if ('uuid' in response):
            uuid = response['uuid']
            if(saveFiles):
                save_json(table,uuid,Dict)
            return(uuid)
        else:
            print("Unexpected response:")
            print(r.text)
            print("Dict:")
            print(Dict)
        return(False)

#post a json encoded record to the fhir/rest interface
def putDict(endpoint, table, Dict, new_uuid):
    Dict['id'] = new_uuid;
    if (endpoint == 'fhir'):
        uri = config['baseuri'] + "/fhir/" + table.capitalize() + "/" + new_uuid
    else:
        uri = config['baseuri']  + "/rest/v1/" + table
    r = requests.put(uri, json=Dict, auth=HTTPBasicAuth(config['OPENMRS_USER'], config['OPENMRS_PASS']))
    if debug:
        print(Dict)
    if ("Location" in r.headers):
        return (r.headers['Location'].split('/').pop())
    elif (r.status_code == 200) :
        print(r.text)
        return (True)
    else:
        print("Unexpected response:")
        print(r.text)
        return(False)

#post a json encoded record to the fhir/rest interface
def delDict(endpoint, table, uuid):
    if (endpoint == 'fhir'):
        uri = config['baseuri'] + "/fhir/" + table.capitalize() + "/" + uuid + '?reason=failedimport'
        r = requests.delete(uri, auth=HTTPBasicAuth(config['OPENMRS_USER'], config['OPENMRS_PASS']))
        print(r)
    return(True)

#load not-yet-imported admissions records for imported patients
def getAdmissions(self, limit):
    pg_cur = openPgCursor(self)
    stmt = "select hadm_id,a.row_id,visittype_uuids.uuid visit_type_uuid,discharge_location_uuids.uuid discharge_location_uuid,admission_location_uuids.uuid admission_location_uuid,patient_uuid,admittime,dischtime,admission_type,visittypes.row_id visit_type_code,admission_location,discharge_location,diagnosis,edregtime,edouttime,deltadate.offset from admissions a left join (select uuid patient_uuid,patients.* from patients left join uuids on patients.row_id = uuids.row_id where uuids.src = 'patients') p  on a.subject_id = p.subject_id left join locations admission_locations on a.admission_location = admission_locations.location left join locations discharge_locations on a.discharge_location = discharge_locations.location left join uuids admission_location_uuids on admission_locations.row_id = admission_location_uuids.row_id  and admission_location_uuids.src = 'locations' left join uuids discharge_location_uuids on discharge_locations.row_id = discharge_location_uuids.row_id  and discharge_location_uuids.src = 'locations' left join visittypes on a.admission_type = visittypes.visittype left join uuids visittype_uuids on visittype_uuids.row_id = visittypes.row_id and visittype_uuids.src = 'visittypes' left join deltadate on deltadate.subject_id = p.subject_id where patient_uuid is not null and a.row_id not in (select row_id from uuids where src = 'admissions')"
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
def getAdmissionData(self, admission):
    self.limit = False
    self.filter =  {'hadm_id': admission.hadm_id}
    tables = ['callout','icustays','services','transfers']
    admission_data = {}
    for table in tables:
        admission_data[table] = []
        self.src = table
        cur = getSrc(self)
        for record in cur:
            admission_data[table].append(record)
    return (admission_data)

# load observation events admission related tables
def getEvents(self, admission):
    child = copy.copy(self)
    child.uuid = -1
    child.limit = False
    print(admission.hadm_id)
    child.filter =  {'hadm_id': admission.hadm_id}
    tables = [
        'chartevents','diagnoses_icd',
        'labevents','inputevents_cv', 'inputevents_mv',
        'noteevents','outputevents',
        'procedureevents_mv','procedures_icd']
    admission_events = {}
    for table in tables:
      if not self.arg or (self.arg == table):
        admission_events[table] = []
        child.src = table
        cur = getSrc(child)
        names = [ x[0] for x in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame( rows, columns=names)
        admission_events[table] = df
    return (admission_events)

# load observation events admission related tables
def getMbEvents(self, admission):
    child = copy.copy(self)
    child.uuid = -1
    child.limit = False
    child.filter =  {'hadm_id': admission.hadm_id}
    child.src = 'microbiologyevents'
    cur = getSrc(child)
    names = [ x[0] for x in cur.description]
    rows = cur.fetchall()
    df = pd.DataFrame( rows, columns=names)
    return (df)

#load generate a uuid array for easy searching
def genIndex(self):
    self.getExisting = True
    self.uuid = 1
    cur = getSrc(self)
    records = {}
    for record in cur:
        records[getattr(record,self.index_on)] = record.uuid;
    return(records)

#load all visits into an array for easy searching
def getAdmissionEncounters(self):
    self.getExisting = True
    self.src ='admissions' 
    self.uuid = 1
    cur = getSrc(self)
    admissions = {}
    for admission in cur:
        admissions[admission.hadm_id] = admission.uuid
    return(admissions)

#load all caregivers into an array for easy searching
def getCaregivers(self):
    self.getExisting = True
    self.src ='caregivers' 
    cur = getSrc(self)
    caregivers = {}
    for caregiver in cur:
        caregivers[caregiver.cgid] = caregiver.uuid
    return(caregivers)

#load all concepts into an array for easy searching
def getConcepts(self):
    self.getExisting = True
    child = copy.copy(self)
    child.src = 'concepts' 
    child.uuid = 1
    cur = getSrc(child)
    concepts = {}
    concepts['test_num'] = {}
    concepts['test_text'] = {}
    concepts['test_enum'] = {}
    concepts['diagnosis'] = {}
    concepts['answer'] = {}
    concepts['category'] = {}
    concepts['diagnoses_icd'] = {}
    concepts['procedure_icd'] = {}
    concepts['ANTIBACTERIUM'] = {}
    concepts['SPECIMEN'] = {}
    concepts['ORGANISM'] = {}
    for concept in cur:
      try:
        if concept.concept_type in ['test_num','test_text','test_enum','SPECIMEN','ORGANISM','ANTIBACTERIUM']:
            concepts[concept.concept_type][concept.itemid] = concept.uuid
        elif concept.concept_type in ['diagnosis','answer','category']:
            concepts[concept.concept_type][concept.shortname] = concept.uuid
        elif concept.concept_type in ['diagnoses_icd','procedure_icd']:
            concepts[concept.concept_type][concept.icd9_code] = concept.uuid
      except Exception as e:
        print(e) 
    return concepts

#MODEL MANIPULATION
#
# insert visit type records into encountertypes table in openmrs db
def visittypestoVisitTypes(self):
    self = copy.copy(self)
    self.x = False
    self.src = 'visittypes'
    et_cur = getSrc(self)
    for record in et_cur:
        date = time.strftime('%Y-%m-%d %H:%M:%S')
        et_uuid = str(uuid.uuid4())
        et_id = insertDict(self,
            'visit_type', {
                "creator": "1",
                "uuid": et_uuid,
                "description": record.visittype,
                "name": record.visittype,
                "date_created": date,
            })
        uuid_cur = insertPgDict(self, 'uuids', {
            'src': src,
            'row_id': record.row_id,
            'uuid': et_uuid
        })
        uuid_cur.close()
    et_cur.close()
    self.pg_conn.commit()

# post practitioners to openmrs fhir interface
def addCaregiver(self,record):
    #self.src = 'caregivers'
    #concept_cur = getSrc(self, limit)
    birthdate = randomDate("1900-01-01", "2000-01-01")
    gender = random.choice(['male', 'female'])
    caregiver = {
            "resourceType": "Practitioner",
            "name": [{
                "family": names.get_last_name(),
                "given": [names.get_first_name(gender=gender)],
                "suffix": [record.label]
            }],
            "gender": gender,
            "birthDate": birthdate,
            "active": True
    }
    if(debug):
        print(caregiver)
    uuid = postDict('fhir', 'Practitioner', caregiver)
    return(uuid)

# delete then load a patient to openmrs fhir interface
def deletePatient(self,subject_id):
    patient = copy.copy(self)
    patient.x = False
    patient.filter = {'subject_id':subject_id}
    admission = copy.copy(patient)
    patient.src = 'patients'
    patient.uuid = 1
    patient_cur = getSrc(patient)
    for record in patient_cur:
        if(record.uuid):
            print("delete patient: " + record.uuid)
            print(record)
            #remove old uuid
            delDict('fhir','patient', record.uuid)
        uuid_cur = deletePgDict(patient, 'uuids', {
           'src': patient.src,
           'row_id': record.row_id,
        })
        uuid_cur.close()
    #delete admissions from uuids table
    patient_cur.close()
    admission.src = 'admissions'
    admission_cur = getSrc(admission)
    for record in admission_cur:
            deletePgDict(admission, 'uuids',{
                'src':admission.src,
                'row_id':record.row_id
            })
            print(record)
    self.pg_conn.commit()

def addPatient(self,record):
    gender = {"M": "male", "F": "female"}[record.gender]
    deceasedBoolean = {1: True, 0: False}[record.expire_flag]
    if(config['USE_OMRSNUM']):
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
    if(debug):
        print(patient)
    #uuid = create(Patient, patient)
    uuid = postDict('fhir', 'Patient', patient)
    return(uuid)

def addEvents(self, admission):
    imported = False
    #check to see if import was previously run
    child_1 = copy.copy(self); 
    child_1.deltadate = False
    child_1.uuid = 0;
    child_1.src = 'process_queue';
    child_1.filter =  {'row_id': admission.row_id, "process_type": "addEvents", 'src': 'visits'}
    cur_1 = getSrc(child_1)
    rows = cur_1.fetchall()
    if len(rows) > 0:
        imported = True 
    if not imported:
        process = {
             "src": "visits",
             "row_id": admission.row_id,
             "process_type": "addEvents",
             "state": 0
        }
        events = getEvents(self, admission)
        for table in events:
            for index, event in events[table].iterrows():
                try:
                    encounter_uuid = uuid_array['icustays'][event.icustay_id]
                except Exception:
                    encounter_uuid = uuid_array['admissions'][event.hadm_id]
                    pass
                try:
                    itemid = event.itemid
                except Exception:
                    itemid = False
                    pass
                try:
                    category = event.itemid
                except Exception:
                    category = False
                    pass
                if(table == 'chartevents' and itemid == 917 and not pd.isna(event.value)):
                    addDiagnosis(admission,event,admission.uuid)
                    addObs(self,table,event,admission,encounter_uuid)
                else:
                    addObs(self,table,event,admission,encounter_uuid)
        insertPgDict(self,'process_queue',process);
        self.pg_conn.commit()
        self.mysql_conn.commit()

def addMbEvents(self, admission):
    events = getMbEvents(self, admission)
    mbReports = mbEvents(events,admission)
    for report in mbReports:
        uuid = postDict('fhir', 'DiagnosticReport', report)
    self.pg_conn.commit()



def printAdmission(self,admission):
        print("processing admission: " + str(admission.hadm_id))
        child = copy.copy(self) 
        admission_data = getAdmissionData(child, admission)
        print(admission_data)


def addVisitObservations(self, visit):
    encounter_uuid = uuid_array['visits'][visit.hadm_id]
    patient_uuid = visit.patient_uuid
    self.fhir_array.append('Patient/'+ patient_uuid)
    self.fhir_array.append('Encounter/'+encounter_uuid)
    report = genFhirBenchmarkObs(self,encounter_uuid,patient_uuid)
    report_uuid = str(uuid.uuid4())
    print(self)
    save_json('DiagnosticReport',report_uuid,report)
    #dr_uuid = postDict('fhir', 'DiagnosticReport', report)
    self.fhir_array.append('DiagnosticReport/'+ dr_uuid)


# post simple admissions to openmrs fhir encounters interface
def addSimpleAdmission(self,record):
        stay_array={}
        print("processing admission: " + str(record.hadm_id))
        child = copy.copy(self) 
        admission_data = getAdmissionData(child, record)
        admit_date = deltaDate(record.admittime, record.offset)
        disch_date = deltaDate(record.dischtime, record.offset)
        # generate grandparent (visit encounter)
        visit = {
            "resourceType":
            "Encounter",
            "status":
            "finished",
            "type": [{
                "coding": [{
                    "code": str(record.visit_type_code),
                    "userSelected": True
                }]
            }],
            "subject": {
                "id": record.patient_uuid,
                "reference": "Patient/" +  record.patient_uuid,
             },
            "period": {
                "start": admit_date,
                "end": disch_date
            },
            "location": [{
                "location": {
                    "reference": "Location/" + record.admission_location_uuid,
                },
                "period": {
                    "start": admit_date,
                    "end": disch_date
                }
            }]
        }
        visit_uuid = postDict('fhir', 'Encounter', visit)
        # generate parent (admission encounter)
        admission = {
            "resourceType":
            "Encounter",
            "status":
            "finished",
            "type": [{
                "coding": [{
                    "display": "Vitals"
                }]
            }],
            "subject": {
                #"reference": "Patient/" + record.patient_uuid
                "id": record.patient_uuid
            },
            "period": {
                "start": admit_date,
                "end": disch_date
            },
            "location": [{
                "location": {
                    "reference": "Location/" + record.admission_location_uuid,
                },
                "period": {
                    "start": admit_date,
                    "end": disch_date
                }
            }],
            "partOf": {
                "reference": "Encounter/" + visit_uuid,
            }
        }
        admission_uuid = postDict('fhir', 'Encounter', admission)
        return(admission_uuid)

# post admissions to openmrs fhir encounters interface
def addAdmission(self,record):
        stay_array={}
        print("processing admission: " + str(record.hadm_id))
        child = copy.copy(self) 
        admission_data = getAdmissionData(child, record)
        admit_date = deltaDate(record.admittime, record.offset)
        disch_date = deltaDate(record.dischtime, record.offset)
        # generate grandparent (visit encounter)
        visit = {
            "resourceType":
            "Encounter",
            "status":
            "finished",
            "type": [{
                "coding": [{
                    "code": str(record.visit_type_code),
                    "userSelected": True
                }]
            }],
            "subject": {
               #"reference": "Patient/" + record.patient_uuid
                "id": record.patient_uuid
            },
            "period": {
                "start": admit_date,
                "end": disch_date
            },
            "location": [{
                "location": {
                    "reference": "Location/" + record.admission_location_uuid,
                },
                "period": {
                    "start": admit_date,
                    "end": disch_date
                }
            }]
        }
        visit_uuid = postDict('fhir', 'Encounter', visit)
        # generate parent (admission encounter)
        admission = {
            "resourceType":
            "Encounter",
            "status":
            "finished",
            "type": [{
                "coding": [{
                    "display": "Vitals"
                }]
            }],
            "subject": {
                #"reference": "Patient/" + record.patient_uuid
                "id": record.patient_uuid
            },
            "period": {
                "start": admit_date,
                "end": disch_date
            },
            "location": [{
                "location": {
                    "reference": "Location/" + record.admission_location_uuid,
                },
                "period": {
                    "start": admit_date,
                    "end": disch_date
                }
            }],
            "partOf": {
                "reference": "Encounter/" + visit_uuid,
            }
        }
        admission_uuid = postDict('fhir', 'Encounter', admission)
        admission_cur = insertPgDict(child,'uuids', {
           'src': 'admissions',
           'row_id': record.row_id,
           'uuid': admission_uuid
        })
        if (admission_uuid == False):
            return(False)
        # add icustays
        for icustay in admission_data['icustays']:
            print("processing stay: " + str(icustay.icustay_id))
            start =  deltaDate(icustay.intime, record.offset)
            if(icustay.outtime is None):
                end = start
            else:
                end = deltaDate(icustay.outtime, record.offset)
            icuenc = {
                "resourceType": "Encounter",
                "status": "finished",
                "type": [{
                    "coding": [{
                        "display": "Vitals"
                    }]
                }],
                "subject": {
                #    "reference": "Patient/" + record.patient_uuid
                    "id": record.patient_uuid
                },
                "period": {
                    "start": start,
                    "end": end
                },
                "location": [{
                    "location": {
                        "reference": "Location/" + uuid_array['locations'][icustay.first_careunit],
                    },
                    "period": {
                        "start": start,
                        "end": end
                    }
                },{
                    "location": {
                        "reference": "Location/" + uuid_array['locations'][icustay.last_careunit],
                    },
                    "period": {
                        "start": start,
                        "end": end
                    }
                },{
                }],
                "partOf": {
                    "reference": "Encounter/" + visit_uuid,
                }
            }
            icuenc_uuid = postDict('fhir', 'Encounter', icuenc)
            icuuuid_cur = insertPgDict(child,'uuids', {
               'src': 'icustays',
               'row_id': icustay.row_id,
               'uuid': icuenc_uuid
            })
            icuuuid_cur.close()
        # handle transfers
        for transfer in admission_data['transfers']:
            transenc = False
            current_unit = False
            previous_unit = False
            transenc = False
            start = False
            end = False
            if(transfer.intime):
                start = deltaDate(transfer.intime, record.offset)
            else:
                continue
            if(transfer.outtime):
                end = deltaDate(transfer.outtime, record.offset)
            else:
                end = start
            if(transfer.curr_careunit):
                current_unit = "Location/" + uuid_array['locations'][transfer.curr_careunit]
            if(transfer.prev_careunit):
                previous_unit = "Location/" + uuid_array['locations'][transfer.prev_careunit]
            if(transfer.eventtype == 'discharge'):
                transfer_type = "Discharge"
            elif(transfer.eventtype == 'admit'):
                transfer_type = "Admission"
            elif(transfer.eventtype == 'transfer'):
                transfer_type = "Transfer"
            else:
                continue
            transenc = {
                "resourceType": "Encounter",
                "status": "finished",
                "subject": {
                   # "reference": "Patient/" + record.patient_uuid
                    "id": record.patient_uuid
                },
                "period": {
                    "start": start,
                    "end": end
                },
                "partOf": {
                    "reference": "Encounter/" + visit_uuid,
                }
            }
            transenc["type"] = [{
                         "coding": [{
                             "display": transfer_type
                        }]
                    }]
            if(previous_unit or current_unit):
                transenc['location'] = []
                if(current_unit):
                    transenc['location'].append({
                        "location": {
                            "reference": current_unit,
                        },
                        "period": {
                            "start": start,
                            "end": end
                        }
                    })
                if(previous_unit):
                    transenc['location'].append({
                        "location": {
                            "reference": previous_unit,
                        },
                        "period": {
                            "start": start,
                            "end": end
                        }
                    })
            transuuid = postDict('fhir', 'Encounter', transenc)
        return(visit_uuid)


#create a fhir observation for a mimic event
def addFhirObs(self,obs_type,obs,admission,encounter_uuid):
    value = False
    units = False
    date = False
    concept_uuid = False
    value_type = False
    if(obs_type in ('outputevents','procedureevents_mv')):
        value_type = 'numeric'
        if not pd.isna(obs.value):
            value = obs.value
        units = obs.valueuom
        concept_uuid = uuid_array['concepts']['test_num'][obs.itemid]
    elif(obs_type in ('inputevents_cv','inputevents_mv')):
        value_type = 'numeric'
        if not pd.isna(obs.amount):
            value = obs.amount
            units = obs.amountuom
        elif not pd.isna(obs.rate):
            value = obs.rate
            units = obs.rateuom
        elif not pd.isna(obs.originalamount):
            value = obs.originalamount
            units = obs.originalamountuom
        elif not pd.isna(obs.originalrate):
            value = obs.originalrate
            units = obs.originalrateuom
        concept_uuid = uuid_array['concepts']['test_num'][obs.itemid]
    elif(obs_type in ('chartevents','labevents')):
        if not pd.isna(obs.valuenum):
            value_type = 'numeric'
            value = obs.valuenum
            concept_uuid = uuid_array['concepts']['test_num'][obs.itemid]
            if(obs.valueuom):
                units = obs.valueuom
        elif not pd.isna(obs.value):
            value_type = 'text'
            value = obs.value
            concept_uuid = uuid_array['concepts']['test_text'][obs.itemid]
    elif(obs_type == 'noteevents'):
        value_type = 'text'
        concept_uuid =uuid_array['concepts']['category'][obs.category]
        value = obs.text
    try:
        if not pd.isna(obs.charttime):
            date = obs.charttime
    except Exception:
        pass
    try:
        if not pd.isna(obs.starttime):
            date = obs.starttime
    except Exception:
        pass
    if not date:
        try:
            if not pd.isna(obs.chartdate):
                date = obs.chartdate
        except Exception:
            pass
    if(concept_uuid and not pd.isna(value) and not value is False and value_type):
        observation = {
            "resourceType": "Observation",
            "code": {
                "coding": [{
                    "system": "http://openmrs.org",
                    "code": concept_uuid
                 }]
            },
            "subject": {
                'reference': 'Patient/' + admission.patient_uuid
            },
            "context": {
                "reference": "Encounter/" + encounter_uuid,
            }
        }
        if(date):
            observation["effectiveDateTime"] =  deltaDate(date, admission.offset);
        if(value_type == 'numeric'):
            observation["valueQuantity"] =  {
               "value": str(value),
               "unit": units ,
               "system": "http://unitsofmeasure.org",
            }
        elif(value_type == 'text'):
            observation["valueString"] = value
        if(create_omrs_obs):
            observation_uuid = postDict('fhir', 'Observation', observation)
            observation_cur = insertPgDict(self,'uuids', {
               'src': obs_type,
               'row_id': obs.row_id,
               'uuid': observation_uuid
            })
            return(observation_uuid)
        else:
            observation_uuid = str(uuid.uuid4())
            save_json('Observation',observation_uuid,observation)
            return(observation_uuid)
    else:
        print('skipping:')
        print([obs_type,concept_uuid,value_type,value,units,date,obs.row_id])
        return(None)
#create a fhir observation for a mimic event
def addObs(self,obs_type,obs,admission,encounter_uuid):
    caregiver_uuid = False
    value = False
    units = False
    date = False
    concept_uuid = False
    value_type = False
    if(obs_type in ('outputevents','procedureevents_mv')):
        value_type = 'numeric'
        if not pd.isna(obs.value):
            value = obs.value
        units = obs.valueuom
        concept_uuid = uuid_array['concepts']['test_num'][obs.itemid]
    elif(obs_type in ('inputevents_cv','inputevents_mv')):
        value_type = 'numeric'
        if not pd.isna(obs.amount):
            value = obs.amount
            units = obs.amountuom
        elif not pd.isna(obs.rate):
            value = obs.rate
            units = obs.rateuom
        elif not pd.isna(obs.originalamount):
            value = obs.originalamount
            units = obs.originalamountuom
        elif not pd.isna(obs.originalrate):
            value = obs.originalrate
            units = obs.originalrateuom
        concept_uuid = uuid_array['concepts']['test_num'][obs.itemid]
    elif(obs_type in ('chartevents','labevents')):
        if not pd.isna(obs.valuenum):
            value_type = 'numeric'
            value = obs.valuenum
            concept_uuid = uuid_array['concepts']['test_num'][obs.itemid]
            if(obs.valueuom):
                units = obs.valueuom
        elif not pd.isna(obs.value):
            value_type = 'text'
            value = obs.value
            concept_uuid = uuid_array['concepts']['test_text'][obs.itemid]
    elif(obs_type == 'noteevents'):
        value_type = 'text'
        concept_uuid =uuid_array['concepts']['category'][obs.category]
        value = obs.text
    elif(obs_type == 'diagnoses_icd'):
        value_type = 'numeric'
        if(obs.icd9_code):
          concept_uuid = uuid_array['concepts']['diagnoses_icd'][obs.icd9_code]
          value = obs.seq_num,
          date = admission.admittime
    try:
        if not pd.isna(obs.charttime):
            date = obs.charttime
    except Exception:
        pass
    try:
        if not pd.isna(obs.starttime):
            date = obs.starttime
    except Exception:
        pass
    try:
        caregiver_uuid = uuid_array['caregivers'][obs.cgid]
    except Exception:
        pass



    if not date:
        try:
            if not pd.isna(obs.chartdate):
                date = obs.chartdate
        except Exception:
            pass
    if(concept_uuid and not pd.isna(value) and not value is False and value_type):
        observation_uuid = str(uuid.uuid4())
        observation = {
                'observation_uuid': observation_uuid,
                'concept_uuid': concept_uuid,
                'patient_uuid': admission.patient_uuid,
                'encounter_uuid': encounter_uuid,
                'caregiver_uuid': caregiver_uuid,
                'src': obs_type,
                'row_id': obs.row_id,
        }
        if(date):
            observation["obs_datetime"] =  deltaDate(date, admission.offset)
        if(value_type == 'numeric'):
            observation["value_numeric"] = value
        elif(value_type == 'text'):
            observation["value_text"] = value
        insertDict(self,'obs_queue',observation);
        return(observation_uuid)
    else:
        print('skipping:')
        print([obs_type,concept_uuid,value_type,value,units,date,obs.row_id])
        return(None)

def addDiagnosis(admission,event,encounter_uuid):
    try:
        cguuid = uuid_array['caregivers'][event.cgid]
    except Exception:
        cguuid = 'd5889800-845c-496c-a637-58c4f7edc953'
    note = {
        "resourceType":
        "Encounter",
        "status":
        "finished",
        "type": [{
            "coding": [{
                "display": "Visit Note"
            }]
        }],
        "subject": {
            'id': admission.patient_uuid
        },
        "period": {
            "start": deltaDate(admission.admittime, admission.offset),
            "end": deltaDate(admission.dischtime, admission.offset)
        },
        "location": [{
            "location": {
                "reference": "Location/" + admission.admission_location_uuid,
            },
            "period": {
                "start": deltaDate(admission.admittime, admission.offset),
                "end": deltaDate(admission.dischtime, admission.offset)
            }
        }],
        "partOf": {
            "reference": "Encounter/" + encounter_uuid,
        },
        "participant": [{
            "individual": {
            "reference": "Practitioner/" + cguuid,
        }
        }]
    }
    encounter_uuid = postDict('fhir', 'Encounter', note)
    certainty_json = {
        "resourceType": "Observation",
        "code": {
            "coding": [
                {
                    "system": "http://openmrs.org",
                    "code": "159394AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                }
            ]
        },
        "performer": [
            {
                "reference": "Practitioner/ef503d24-800b-431f-8ebd-799b8018cc8b",
            }
        ],
        "subject": {
            "reference": "Patient/" + admission.patient_uuid
        },
        "context": {
            "reference": "Encounter/" + encounter_uuid,
        },
        "effectiveDateTime":  deltaDate(event.charttime,admission.offset),
        "valueCodeableConcept": {
           "coding": [
                {
                    "system": "http://openmrs.org",
                    "code": "159393AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                }
            ]
        }
    }
    diagnosis_json = {
        "resourceType": "Observation",
        "status": "final",
        "code": {
            "coding": [
                {
                    "system": "http://openmrs.org",
                    "code": "161602AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                }
            ]
        },
        "performer": [
            {
                "reference": "Practitioner/ef503d24-800b-431f-8ebd-799b8018cc8b",
            }
        ],
        "subject": {
            "reference": "Patient/" + admission.patient_uuid
        },
        "context": {
            "reference": "Encounter/" + encounter_uuid,
        },
        "effectiveDateTime":  deltaDate(event.charttime,admission.offset),
        "valueString": event.value 
    }
    order_json = {
        "resourceType": "Observation",
        "status": "final",
        "code": {
            "coding": [
                {
                    "system": "http://openmrs.org",
                    "code": "159946AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                }
            ]
        },
        "performer": [
            {
                "reference": "Practitioner/ef503d24-800b-431f-8ebd-799b8018cc8b",
            }
        ],
        "subject": {
            "reference": "Patient/" + admission.patient_uuid
        },
        "context": {
            "reference": "Encounter/" + encounter_uuid,
        },
        "effectiveDateTime":  deltaDate(event.charttime,admission.offset),
        "valueCodeableConcept": {
            "coding": [
                {
                    "system": "http://openmrs.org",
                    "code": "159943AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                }
            ]
        }
    }
    order_uuid = postDict('fhir', 'Observation', order_json)
    diagnosis_uuid = postDict('fhir','Observation',diagnosis_json)
    certainty_uuid = postDict('fhir','Observation',certainty_json)
    obsgroup_json = {
        "resourceType": "Observation",
        "code": {
            "coding": [{
                    "system": "http://openmrs.org",
                    "code": "159947AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            }]
        },
        "performer": [
            {
                "reference": "Practitioner/ef503d24-800b-431f-8ebd-799b8018cc8b",
            }
        ],
        "subject": {
            "reference": "Patient/" + admission.patient_uuid
        },
        "context": {
            "reference": "Encounter/" + encounter_uuid,
        },
        "effectiveDateTime":  deltaDate(event.charttime,admission.offset),
        "valueString": '',
        "related": [
            {
                "type": "has-member",
                "target": {
                    "reference": "Observation/" + certainty_uuid,
                }
            },
            {
                "type": "has-member",
                "target": {
                    "reference": "Observation/" + diagnosis_uuid,
                }
            },
            {
                "type": "has-member",
                "target": {
                    "reference": "Observation/" + order_uuid,
                }
            }
        ]
    }
    obsgroup_uuid = postDict('fhir','Observation',obsgroup_json)

# link enumerated concepts to their parent concepts
def genConceptMap(self):
    concept_cur = getConceptMap(self)
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
        insertDict(self,'concept_answer',concept_answer);
    concept_cur.close()
    self.pg_conn.commit()

# post locations to openmrs fhir interface
def locationsToLocations(self):
    self.src = 'locations'
    locations_cur = getSrc(self)
    for record in locations_cur:
        uuid = postDict(
            'fhir',
            'Location',
            {
                "resourceType": "Location",
                #        "id": record.uuid,
                "name": record.location,
                "description": record.location,
                "status": "active"
            })
        uuid_cur = insertPgDict(self, 'uuids', {
            'src': self.src,
            'row_id': record.row_id,
            'uuid': uuid
        })
        uuid_cur.close()
    locations_cur.close()
    self.pg_conn.commit()

# post encounter types to openmrs rest interface
def postEncounterTypes(self):
    self.src = 'encountertypes'
    et_cur = getSrc(self)
    for record in et_cur:
        uuid=postDict(
            'rest',
            'Encountertype',
            {
                "name": record.encountertype,
                "description": record.encountertype,
            })
        uuid_cur = insertPgDict(self, 'uuids', {
            'src': self.src,
            'row_id': record.row_id,
            'uuid': uuid
        })
        uuid_cur.close()
    et_cur.close()
    self.pg_conn.commit()

# post visit types to openmrs rest interface
def postVisitTypes(self):
    self.src = 'visittypes'
    et_cur = getSrc(self)
    for record in et_cur:
        uuid=postDict(
            'rest',
            'visittype',
            {
                "name": record.visittype,
                "description": record.visittype,
            })
        uuid_cur = insertPgDict(self,'uuids', {
            'src': self.src,
            'row_id': record.row_id,
            'uuid': uuid
        })
        uuid_cur.close()
    et_cur.close()
    self.pg_conn.commit()

# insert concepts into openmrs concepts and related tables
def conceptsToConcepts(self):
    self.src = 'concepts'
    #change the auto_increment value so we don't step on built-in concepts
    setIncrementer(self, 'concept','166000')
    setIncrementer(self, 'concept_name','166000')
    setIncrementer(self, 'concept_description','166000')
    concept_cur = getSrc(self)
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
        concept_id = insertDict(self, 'concept',concept)
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
        insertDict(self, 'concept_name',concept_name_1)
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
        insertDict(self, 'concept_name',concept_name_2)
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
        insertDict(self, 'concept_description',concept_description)
        if record.avg_val:
            numeric={
                    "concept_id": concept_id,
                    "precise": "1",
                    "hi_absolute":record.max_val,
                    "low_absolute":record.min_val
                }
            if record.units:
                numeric["units"]=record.units
            insertDict(self, 'concept_numeric',numeric)
        elif record.concept_type == 'test_num':
            numeric={
                    "concept_id": concept_id,
                    "precise": "1",
                }
            insertDict(self, 'concept_numeric',numeric)
        uuid_cur = insertPgDict(self, 'uuids', {
            'src': self.src,
            'row_id': record.row_id,
            'uuid': concept_uuid
        })
        uuid_cur.close()
        update_cur = updatePgDict(self, 'concepts', {
            'openmrs_id': concept_id,
        },{
            'row_id': record.row_id,
        })
        update_cur.close()
    concept_cur.close()
    setIncrementer(self, 'concept','3')
    setIncrementer(self, 'concept_name','21')
    setIncrementer(self, 'concept_description','1')
    self.pg_conn.commit()

#post a json encoded record to the fhir/rest interface
def mbEvents(df,admission):
    encounter_uuid = uuid_array['visits'][admission.hadm_id]
    #set charttime to chartdate + 00:00:00 where blank
    mask = pd.isna(df['charttime'])
    df['charttime_imp'] = df['charttime']
    df.loc[mask, 'charttime_imp'] = df['chartdate']
    #iterate over specimen-charttimes
    specimens = df.groupby(['spec_itemid', 'charttime_imp','spec_type_desc'], as_index=False).sum()
    specs = []
    for index, s in specimens.iterrows():
         spec_uuid = None
         try:
             spec_uuid = uuid_array['concepts']['SPECIMEN'][int(s.spec_itemid)]
         except Exception as e:
             print('epec error')
             print(e)
         #get original data for this spec
         df_spec = df[(df.charttime_imp == s.charttime_imp) & (df.spec_itemid == s.spec_itemid)]
         org = df_spec.groupby(['org_itemid','org_name'], as_index=False).sum()
         events = []
         for i2, o in org.iterrows():
             df_org = df[(df.charttime_imp == s.charttime_imp) & (df.spec_itemid == s.spec_itemid) & (df.org_itemid == o.org_itemid)]
             resp = []
             for i3, a in df_org.iterrows():
                 antibiotic_uuid = None
                 try:
                     antibiotic_uuid = uuid_array['concepts']['ANTIBACTERIUM'][int(a.ab_itemid)]
                 except Exception:
                     pass
                 resp.append({'isolate_num':a.isolate_num,'dilution_comparison':a.dilution_comparison,'dilution_value':a.dilution_value,'dilution_text':a.dilution_text,'ab_name':a.ab_name,'ab_itemid':a.ab_itemid,'antibiotic_uuid':antibiotic_uuid,'interpretation':a.interpretation})
             organism_uuid = None
             try:
                 organism_uuid = uuid_array['concepts']['ORGANISM'][int(o.org_itemid)]
             except Exception:
                 pass
             events.append({'org_itemid':o.org_itemid,'organism_uuid':organism_uuid,'org_name':o.org_name,'resp':resp})
         charttime = deltaDate(s.charttime_imp, admission.offset)
         specs.append({'patient_uuid':admission.patient_uuid,'admission_uuid':admission.uuid,'charttime':charttime,'spec_itemid':s.spec_itemid,'spec_type_desc':s.spec_type_desc,'spec_uuid':spec_uuid,'events':events})

    diag_reports = []
    for record in specs:
        i=0
        if len(record['events']) > 0:
          growth_detected = 'POS'
        else:
          growth_detected = 'NEG'
        interpretation = {
            "coding": [
                {
                    "system": "http://hl7.org/fhir/v2/0078",
                    "code": growth_detected
                }
            ]
        }
        report = {
            'resourceType': 'DiagnosticReport',
            'status': 'final',
            'issued': record['charttime'],
            'effectiveDateTime':  record['charttime'],
            'category': {
                'coding': [{
                    'system': 'http://hl7.org/fhir/v2/0074',
                    'code': 'MB'
                 }]
            },
            'subject':{
                'reference': 'Patient/' + record['patient_uuid']
            },
            'contained': [],
            'result': [{
                'reference': "#g0"
            }],
            "context": {
                "reference": "Encounter/" + encounter_uuid,
            }
        }
        g = {
            'resourceType': "Observation",
            'id': 'g0',
                'issued': record['charttime'],
                'effectiveDateTime':  record['charttime'],
                'status': 'final',
                'code': {
                    'coding': [{
                        'system': 'http://openmrs.org',
                        'code': record['spec_uuid']
                     }],
                    'text': record['spec_type_desc']
                },
                'subject': {
                    'reference': 'Patient/' + record['patient_uuid']
                },
                'interpretation': interpretation
        }
        g_related = []
        if len(record['events']) > 0:
            #create a report for each detected organism
            for event in record['events']:
                i+=1
                obs_id = 'org' + str(i);
                org_obs = {
                    'resourceType': 'Observation',
                    'id': obs_id,
                    'code': {
                        'coding': [{
                            'system': 'http://openmrs.org',
                            "code": event['organism_uuid']
                         }],
                        "text": event['org_name']
                     }
                }
                g_related.append({
                    'type': "has-member",
                    'target': {
                        'reference': "#" + obs_id
                    }
                })
                j = 0
                org_related = []
                for agent in event['resp']:
                  if not agent['antibiotic_uuid'] == None:
                    j+=1
                    agent_id = 'org' + str(i) + '_agent' + str(j);
                    dilution_text = agent['dilution_text']
                    if dilution_text is None:
                      dilution_text = 'NA'
                    agent_obs = {
                        "resourceType": "Observation",
                        "status": "final",
                        "id": agent_id,
                        "code": {
                            "coding": [{
                                "system": "http://openmrs.org",
                                "code": agent['antibiotic_uuid']
                             }],
                            "text": agent['ab_name']
                        },
                        'subject': {
                            'reference': "Patient/"+ record['patient_uuid']
                        },
                        'valueString': dilution_text,
                        "interpretation": {
                            "coding": [{
                                "system": "http://hl7.org/fhir/v2/0078",
                                "code": agent['interpretation']
                        }]}
                    }
                    org_related.append({
                        'type': "has-member",
                        'target': {
                            'reference': "#" + agent_id
                        }
                    })
                    report['contained'].append(agent_obs)
                if len(org_related) > 0:
                    org_obs['related'] = org_related 
                report['contained'].append(org_obs)
        if len(g_related) > 0:
            g['related'] = g_related 
        report['contained'].append(g)
        diag_reports.append(report)
    return(diag_reports)
#post a json encoded record to the fhir/rest interface
def mbEvents(df,admission):
    encounter_uuid = uuid_array['visits'][admission.hadm_id]
    #set charttime to chartdate + 00:00:00 where blank
    mask = pd.isna(df['charttime'])
    df['charttime_imp'] = df['charttime']
    df.loc[mask, 'charttime_imp'] = df['chartdate']
    #iterate over specimen-charttimes
    specimens = df.groupby(['spec_itemid', 'charttime_imp','spec_type_desc'], as_index=False).sum()
    specs = []
    for index, s in specimens.iterrows():
         spec_uuid = None
         try:
             spec_uuid = uuid_array['concepts']['SPECIMEN'][int(s.spec_itemid)]
         except Exception as e:
             print('epec error')
             print(e)
         #get original data for this spec
         df_spec = df[(df.charttime_imp == s.charttime_imp) & (df.spec_itemid == s.spec_itemid)]
         org = df_spec.groupby(['org_itemid','org_name'], as_index=False).sum()
         events = []
         for i2, o in org.iterrows():
             df_org = df[(df.charttime_imp == s.charttime_imp) & (df.spec_itemid == s.spec_itemid) & (df.org_itemid == o.org_itemid)]
             resp = []
             for i3, a in df_org.iterrows():
                 antibiotic_uuid = None
                 try:
                     antibiotic_uuid = uuid_array['concepts']['ANTIBACTERIUM'][int(a.ab_itemid)]
                 except Exception:
                     pass
                 resp.append({'isolate_num':a.isolate_num,'dilution_comparison':a.dilution_comparison,'dilution_value':a.dilution_value,'dilution_text':a.dilution_text,'ab_name':a.ab_name,'ab_itemid':a.ab_itemid,'antibiotic_uuid':antibiotic_uuid,'interpretation':a.interpretation})
             organism_uuid = None
             try:
                 organism_uuid = uuid_array['concepts']['ORGANISM'][int(o.org_itemid)]
             except Exception:
                 pass
             events.append({'org_itemid':o.org_itemid,'organism_uuid':organism_uuid,'org_name':o.org_name,'resp':resp})
         charttime = deltaDate(s.charttime_imp, admission.offset)
         specs.append({'patient_uuid':admission.patient_uuid,'admission_uuid':admission.uuid,'charttime':charttime,'spec_itemid':s.spec_itemid,'spec_type_desc':s.spec_type_desc,'spec_uuid':spec_uuid,'events':events})

    diag_reports = []
    for record in specs:
        i=0
        if len(record['events']) > 0:
          growth_detected = 'POS'
        else:
          growth_detected = 'NEG'
        interpretation = {
            "coding": [
                {
                    "system": "http://hl7.org/fhir/v2/0078",
                    "code": growth_detected
                }
            ]
        }
        report = {
            'resourceType': 'DiagnosticReport',
            'status': 'final',
            'issued': record['charttime'],
            'effectiveDateTime':  record['charttime'],
            'category': {
                'coding': [{
                    'system': 'http://hl7.org/fhir/v2/0074',
                    'code': 'MB'
                 }]
            },
            'subject':{
                'reference': 'Patient/' + record['patient_uuid']
            },
            'contained': [],
            'result': [{
                'reference': "#g0"
            }],
            "context": {
                "reference": "Encounter/" + encounter_uuid,
            }
        }
        g = {
            'resourceType': "Observation",
            'id': 'g0',
                'issued': record['charttime'],
                'effectiveDateTime':  record['charttime'],
                'status': 'final',
                'code': {
                    'coding': [{
                        'system': 'http://openmrs.org',
                        'code': record['spec_uuid']
                     }],
                    'text': record['spec_type_desc']
                },
                'subject': {
                    'reference': 'Patient/' + record['patient_uuid']
                },
                'interpretation': interpretation
        }
        g_related = []
        if len(record['events']) > 0:
            #create a report for each detected organism
            for event in record['events']:
                i+=1
                obs_id = 'org' + str(i);
                org_obs = {
                    'resourceType': 'Observation',
                    'id': obs_id,
                    'code': {
                        'coding': [{
                            'system': 'http://openmrs.org',
                            "code": event['organism_uuid']
                         }],
                        "text": event['org_name']
                     }
                }
                g_related.append({
                    'type': "has-member",
                    'target': {
                        'reference': "#" + obs_id
                    }
                })
                j = 0
                org_related = []
                for agent in event['resp']:
                  if not agent['antibiotic_uuid'] == None:
                    j+=1
                    agent_id = 'org' + str(i) + '_agent' + str(j);
                    dilution_text = agent['dilution_text']
                    if dilution_text is None:
                      dilution_text = 'NA'
                    agent_obs = {
                        "resourceType": "Observation",
                        "status": "final",
                        "id": agent_id,
                        "code": {
                            "coding": [{
                                "system": "http://openmrs.org",
                                "code": agent['antibiotic_uuid']
                             }],
                            "text": agent['ab_name']
                        },
                        'subject': {
                            'reference': "Patient/"+ record['patient_uuid']
                        },
                        'valueString': dilution_text,
                        "interpretation": {
                            "coding": [{
                                "system": "http://hl7.org/fhir/v2/0078",
                                "code": agent['interpretation']
                        }]}
                    }
                    org_related.append({
                        'type': "has-member",
                        'target': {
                            'reference': "#" + agent_id
                        }
                    })
                    report['contained'].append(agent_obs)
                if len(org_related) > 0:
                    org_obs['related'] = org_related 
                report['contained'].append(org_obs)
        if len(g_related) > 0:
            g['related'] = g_related 
        report['contained'].append(g)
        diag_reports.append(report)
    return(diag_reports)


def genFhirBenchmarkObs(self,encounter_uuid,patient_uuid):
    date=str((datetime(2000, 1, 1, 0, 0)).isoformat())
    report = {
        'resourceType': 'DiagnosticReport',
        'status': 'final',
        'issued': date,
        'effectiveDateTime':  date,
        'category': {
            'coding': [{
                'system': 'http://hl7.org/fhir/v2/0074',
                'code': 'ICU'
             }]
        },
        'subject':{
            'id':  patient_uuid,
        },
        'contained': [],
        'result': [],
        "context": {
            "reference": "Encounter/" + encounter_uuid,
        }
    }
    i=0
    for step in self.observations:
        i += 1
        obs_id = str(i)
        observation = {
            "resourceType": "Observation",
            "effectiveDateTime": step['date'],
            "id": obs_id,
            "code": {
                "coding": [{
                    "system": "http://openmrs.org",
                    "code": step['concept_uuid']
                 }]
            },
            "subject": {
                'id':  patient_uuid,
                'reference': 'Patient/' + patient_uuid,
                'identifier': {
                     'id':  patient_uuid,
                 }
            },
            "context": {
                "reference": "Encounter/" + encounter_uuid,
            }
        }
        if(step['value_type'] == 'numeric'):
            observation["valueQuantity"] =  {
               "value": str(step['value']),
               "unit": step['units'],
               "system": "http://unitsofmeasure.org",
            }
        elif(step['value_type'] == 'text'):
            observation["valueString"] = step['value']
        report['contained'].append(observation)
        report['result'].append({
                'reference': "#" + obs_id
       })
#    print(report)
    return(report)
