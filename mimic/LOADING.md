# Representing MIMIC-III Data in OpenMRS
![alt text](https://github.com/EpistasisLab/mrsman/blob/master/docs/process.png "Loading Process")
![alt text](https://github.com/EpistasisLab/mrsman/blob/master/docs/graph.png "MIMIC/OpenMRS object map")
# About the script
import.py configures OpenMRS for use with the MIMIC-III dataset. Do not log into OpenMRS until *after* initDb finishes.
# Use:
1. Follow postgres based [mimic build instructions](https://github.com/EpistasisLab/mimic-code/tree/master/buildmimic/postgres)
2. Follow sdk based [OpenMRS installation instructions](https://wiki.openmrs.org/display/docs/OpenMRS+SDK)
3. Update config.json to suit environment
4. Generate metadata, insert concepts into OpenMRS db, create views
```bash
./python/import.py initDb
```
5. Log into OpenMRS
- Navigate to Maitenance->Advanced Settings, and set "validation.disable" to "true"
6. Import records
```bash
./python/import.py initRestResources
./python/import.py initCaregivers
./python/import.py initPatients
./python/import.py initAdmit
./python/import.py genEvents
```



# initDB (20-30 min)
generate MIMIC-III metadata schema, insert OpenMRS concept records
# initRestResources (2-3 seconds)
configure locations, encounter types, visit types
# initCaregivers (5-10 min)
post practitioner records with randomly generated names and birthdates.
# initPatients (1-2 hrs)
Post patient records.  Generate random name, create offset record setting initial encounter timestamp to 2000-01-01 00:00:00 
# initAdmit (2-3 hrs)
Post encounter records for initialized patients
# genEvents (4-6 weeks)
Post obsevation records for initialized admission encounters
