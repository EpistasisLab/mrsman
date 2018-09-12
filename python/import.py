#!/usr/bin/env python3
import sys
import mrsman;
class base ():
  def __init__(self):
    if (len(sys.argv) > 1):
        #run function from cli
        a = eval("base." + sys.argv[1])
        self.exitFlag = 0
        a(self)
    else:
        tmp = globals().copy()
        print("available arguments:")
        print(dir(base))
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
    print("add encounter and visit types")
    mrsman.postEncounterTypes(self)
    mrsman.postVisitTypes(self)
    mrsman.shutdown(self)
  #fhir
  def initCaregivers(self):
    mrsman.bootstrap(self)
    mrsman.caregiversToPractitioners(self,None)
    mrsman.shutdown(self)
  #fhir
  def initPatients(self):
    try:
        num = int(eval(sys.argv[2]))
    except:
        num = 1
    mrsman.exitFlag = False
    src = 'mimiciii.patients'
    adder = 'addPatient'
    try:
        mrsman.splitTask(num,src,adder)
    except (KeyboardInterrupt, SystemExit):
        print('\n! Received keyboard interrupt, quitting threads.\n')
        mrsman.exitFlag = True
  #fhir
  def initAdmit(self):
    try:
        num = int(eval(sys.argv[2]))
    except:
        num = 1
    mrsman.bootstrap(self)
    mrsman.getUuids(self)
    mrsman.exitFlag = False
    src = 'kate.combined_admissions'
    adder = 'addAdmission'
    try:
        mrsman.splitTask(num,src,adder)
    except (KeyboardInterrupt, SystemExit):
        print('\n! Received keyboard interrupt, quitting threads.\n')
  #fhir
  def reinitPatient(self):
    mrsman.bootstrap(self)
    mrsman.getUuids(self)
    subject_id = sys.argv[2]
    mrsman.reloadPatient(subject_id)
    mrsman.shutdown(self)
#
base()
