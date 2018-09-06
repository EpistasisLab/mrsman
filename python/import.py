#!/usr/bin/env python3
import sys
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
def initAdmit():
    getUuids()
    admissionsToEncounters(None)

#fhir
def reinitPatient():
    getUuids()
    subject_id = sys.argv[2]
    reloadPatient(subject_id)

#MAIN
#
#connect to mimic dataset postgres
if (len(sys.argv) > 1):
    from mrsman import *
    bootstrap()
    #run function from cli
    a = eval(sys.argv[1])
    a()
    shutdown()
else:
    tmp = globals().copy()
    print("available arguments:")
    [print(k) for k, v in tmp.items() if not k.startswith('_') and k != 'sys']
