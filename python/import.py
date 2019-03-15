#!/usr/bin/env python3
import sys
import mrsman
import copy
class base ():
  def __init__(self):
    if (len(sys.argv) > 1):
        #run function from cli
        func = False
        num = False
        arg = False
        debug = False
        i = 1
        while i < len(sys.argv):
            if(sys.argv[i].isdigit()):
                num = int(eval(sys.argv[i]))
            elif(sys.argv[i] == '-d'):
                debug = True
            elif(sys.argv[i] == '-a'):
                mrsman.config_file = 'ann.json'
            elif(i > 1 and sys.argv[i].isalpha()):
                arg = sys.argv[i]
            else:
                func = eval("base." + sys.argv[i])
            i += 1
        self.num = num
        self.arg = arg
        mrsman.debug = debug
        if(func):
            func(self)
    else:
        tmp = globals().copy()
        print("available arguments:")
        [print(k) for k in dir(base) if not k.startswith('_') and k != 'sys']
  #
  #Operations
  #
  #initialize openmrs database (run before initial website load on fresh install)
  def initDb(self):
    print("bootstrap")
    mrsman.bootstrap(self)
    print("initialize database")
    mrsman.loadPgsqlFile(self,'../mimic/sql/add_tables.sql')
    mrsman.loadMySQLFile(self,'./mimic/sql/add_tables_mysql.sql')
    print("import concepts")
    mrsman.conceptsToConcepts(self)
    print("link mapped concepts")
    mrsman.genConceptMap(self)
    print("please visit http://"+mrsman.config['IP']+":"+mrsman.config['OPENMRS_PORT']+"/openmrs")
    mrsman.shutdown(self)
  #
  #rest based record creation
  def initRestResources(self):
    print("bootstrap")
    mrsman.bootstrap(self)
    print("add locations")
    mrsman.locationsToLocations(self)
    print("add encounter types")
    mrsman.postEncounterTypes(self)
    print("add visit types")
    mrsman.postVisitTypes(self)
    print("close connections")
    mrsman.shutdown(self)
  #
  #fhir based practitioners
  def initCaregivers(self):
    print("bootstrap")
    mrsman.bootstrap(self)
    self.src = 'caregivers'
    self.uuid = -1
    self.callback = mrsman.addCaregiver
    mrsman.numThreads = 5
    if (not self.num):
       self.num = 8000 
    try:
        mrsman.runTask(self)
    except (KeyboardInterrupt, SystemExit):
        mrsman.exitFlag = True
  #
  #fhir based patient
  def initPatient(self):
    mrsman.bootstrap(self)
    self.deltadate = True
    self.src = 'patients'
    self.uuid = -1
    self.callback = mrsman.addPatient
    mrsman.numThreads = 1
    if(self.num):
        mrsman.numThreads = 1
        self.filter = {'patients.subject_id':self.num}
    try:
        mrsman.runTask(self)
    except (KeyboardInterrupt, SystemExit):
        print('\n! Received keyboard interrupt, quitting threads.\n')
        mrsman.exitFlag = True
  #
  #fhir based patients
  def initPatients(self):
    mrsman.bootstrap(self)
    self.deltadate = True
    self.src = 'patients'
    self.uuid = -1
    self.callback = mrsman.addPatient
    mrsman.numThreads = 5
    try:
        mrsman.runTask(self)
    except (KeyboardInterrupt, SystemExit):
        print('\n! Received keyboard interrupt, quitting threads.\n')
        mrsman.exitFlag = True
  #
  #fhir based admissions
  def initAdmit(self):
    mrsman.getUuids(self)
    self.deltadate = True
    self.src = 'visits'
    self.uuid = -1
    self.callback = mrsman.addAdmission
    try:
        mrsman.runTask(self)
    except (KeyboardInterrupt, SystemExit):
        mrsman.exitFlag = True
        print('\n! Received keyboard interrupt, quitting threads.\n')
  #
  #fhir based observations
  def genEvents(self):
    mrsman.getUuids(self)
    self.deltadate = True
    self.uuid = 2
    self.src = 'visits'
    self.callback = mrsman.addEvents
    if(self.num):
        mrsman.numThreads = 1
        self.filter = {'hadm_id':self.num}
    else:
        mrsman.numThreads = 50
    self.num = False
    mrsman.runTask(self)
  #
  #fhir based observations
  def genMbEvents(self):
    mrsman.getUuids(self)
    self.deltadate = True
    self.uuid = 2
    self.src = 'visits'
    self.callback = mrsman.addMbEvents
    if(self.num):
        mrsman.numThreads = 1
        self.filter = {'hadm_id':self.num}
    else:
        mrsman.numThreads = 50
    self.num = False
    mrsman.runTask(self)
  #
  #fhir delete a patient 
  def deletePatient(self):
    mrsman.getUuids(self)
    subject_id = self.num
    mrsman.deletePatient(self,subject_id)
    mrsman.shutdown(self)

base()
