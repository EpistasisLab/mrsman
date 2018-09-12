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
#        try:
#            a(self)
#        except (KeyboardInterrupt, SystemExit):
#            print('\n! Received keyboard interrupt, quitting threads.\n')
#            exitFlag = 1
    else:
        tmp = globals().copy()
        print("available arguments:")
        [print(k) for k in dir(base) if not k.startswith('_') and k != 'sys']
  #TASKS
  #
  #initialize openmrs database (run before initial website load on fresh install)
  def initDb(self):
    mrsman.bootstrap(self)
    print("initializing database")
    mrsman.loadPgsqlFile('../mimic/sql/add_tables.sql')
    mrsman.shutdown(self)
  #
  def initConcepts(self):
    mrsman.bootstrap(self)
    print("import concepts")
    mrsman.conceptsToConcepts()
    print("link mapped concepts")
    mrsman.genConceptMap()
    mrsman.shutdown(self)
  #rest based record creation
  def initRestResources():
    mrsman.bootstrap(self)
    mrsman.locationsToLocations()
    mrsman.postEncounterTypes()
    mrsman.postVisitTypes()
    mrsman.shutdown(self)
  #fhir
  def initCaregivers():
    mrsman.bootstrap(self)
    mrsman.caregiversToPractitioners(None)
    mrsman.shutdown(self)
  #fhir
  def initPatients(self):
    try:
        num = int(eval(sys.argv[2]))
    except:
        num = 1
    mrsman.exitFlag = False
    try:
        mrsman.splitTask(num,mrsman.loadPatients)
    except (KeyboardInterrupt, SystemExit):
        print('\n! Received keyboard interrupt, quitting threads.\n')
        mrsman.exitFlag = True
  #        patientsToPatients('1')
  #        admissionsToEncounters(None)
  #fhir
  def initAdmit(self):
    mrsman.bootstrap(self)
    mrsman.getUuids()
    mrsman.admissionsToEncounters(None)
    mrsman.shutdown(self)
  #fhir
  def reinitPatient(self):
    mrsman.bootstrap(self)
    mrsman.getUuids()
    subject_id = sys.argv[2]
    mrsman.reloadPatient(subject_id)
    mrsman.shutdown(self)
#
base()
