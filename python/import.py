#!/usr/bin/env python3
import sys
import mrsman;
class base ():
  def __init__(self):
    if (len(sys.argv) > 1):
        #run function from cli
        a = eval("base." + sys.argv[1])
        try:
            self.num = int(eval(sys.argv[2]))
        except:
            self.num = False
        #self.exitFlag = 0
        a(self)
    else:
        tmp = globals().copy()
        print("available arguments:")
        [print(k) for k in dir(base) if not k.startswith('_') and k != 'sys']
  #
  #Operations
  #
  #initialize openmrs database (run before initial website load on fresh install)
  def initDb(self):
    mrsman.bootstrap(self)
    print("initializing database")
    mrsman.loadPgsqlFile(self,'../mimic/sql/add_tables.sql')
    print("import concepts")
    mrsman.conceptsToConcepts(self)
    print("link mapped concepts")
    mrsman.genConceptMap(self)
    print("please visit http://"+mrsman.config['IP']+":"+mrsman.config['OPENMRS_PORT']+"/openmrs")
    mrsman.shutdown(self)
  #
  #rest based record creation
  def initRestResources(self):
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
    mrsman.bootstrap(self)
    self.src = 'mimiciii.caregivers'
    self.uuid = -1
    self.task = mrsman.addRecords
    self.adder = mrsman.addCaregiver
    mrsman.numThreads = 5
    if (not self.num):
       self.num = 8000 
    try:
        mrsman.runTask(self)
    except (KeyboardInterrupt, SystemExit):
        mrsman.exitFlag = True
  #
  #fhir based patients
  def initPatients(self):
    mrsman.bootstrap(self)
    self.deltadate = True
    self.src = 'mimiciii.patients'
    self.uuid = -1
    self.task = mrsman.addRecords
    self.adder = mrsman.addPatient
#    if (not self.num):
#       self.num = 1
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
    self.uuid = -1
    self.src = 'combined_admissions'
    self.task = mrsman.addRecords
    self.adder = mrsman.addAdmission
    try:
        mrsman.runTask(self)
    except (KeyboardInterrupt, SystemExit):
        mrsman.exitFlag = True
        print('\n! Received keyboard interrupt, quitting threads.\n')
  #
  #fhir enchanced observations
  def genDiagnosis(self):
    mrsman.getUuids(self)
    self.deltadate = True
    self.uuid = 1
    mrsman.numThreads = 1
    self.src = 'combined_admissions'
    self.task = mrsman.addRecords
    self.adder = mrsman.addDiag
    mrsman.runTask(self)
  #
  #fhir delete a patient 
  def reinitPatient(self):
    mrsman.getUuids(self)
    subject_id = self.num
    mrsman.deletePatient(self,subject_id)
    mrsman.shutdown(self)

base()
