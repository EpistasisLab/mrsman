# Representing MIMIC-III Data in OpenMRS
## Loading Process
![alt text](https://github.com/EpistasisLab/mrsman/blob/master/docs/process.png "Loading Process")
## Object Map
![alt text](https://github.com/EpistasisLab/mrsman/blob/master/docs/graph.png "MIMIC/OpenMRS object map")
## About the script
import.py configures OpenMRS for use with the MIMIC-III dataset. 
### Usage
1. Follow postgres based [mimic build instructions](https://github.com/EpistasisLab/mimic-code/tree/master/buildmimic/postgres)
2. Follow sdk based [OpenMRS installation instructions](https://wiki.openmrs.org/display/docs/OpenMRS+SDK)
3. Update config.json to suit environment
4. Generate metadata, insert concepts into OpenMRS db, create views
```bash
./python/import.py initDb
```
Do not log into OpenMRS until *after* initDb finishes.
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
### Steps
#### initDB (20-30 min)
generate MIMIC-III metadata schema, insert OpenMRS concept records
#### initRestResources (2-3 seconds)
configure locations, encounter types, visit types
#### initCaregivers (5-10 min)
post practitioner records with randomly generated names and birthdates.
#### initPatients (1-2 hrs)
post patient records.  Generate random name, create offset record setting initial encounter timestamp to 2000-01-01 00:00:00 
#### initAdmit (2-3 hrs)
post encounter records for initialized patients
#### genEvents (4-6 weeks)
post obsevation records for initialized admission encounters

### Counts
| schema |      tablename|      num_records
| ------------- |:-------------:| -----:|
meta |  concepts |      44,615
meta |  encountertypes |        4
meta |  locations |     54
meta |  visittypes |    4
mimiciii |      admissions |    58,976
mimiciii |      callout |  34,499 
mimiciii |      caregivers |    7,567
mimiciii |      chartevents |   330,714,033
mimiciii |      cptevents |     573,146
mimiciii |      d_cpt | 134
mimiciii |      d_icd_diagnoses |       14,567
mimiciii |      d_icd_procedures | 3,882
mimiciii |      d_items |       12,487
mimiciii |      d_labitems |    753
mimiciii |      datetimeevents |        4,486,425
mimiciii |      diagnoses_icd | 651,047
mimiciii |      drgcodes |      125,557
mimiciii |      icustays |      61,532
mimiciii |      inputevents_cv |        17,527,936
mimiciii |      inputevents_mv |        3,619,423
mimiciii |      labevents |     27,860,624
mimiciii |      microbiologyevents |    631,726
mimiciii |      noteevents |    2,078,216
mimiciii |      outputevents |  4,349,263
mimiciii |      patients |      46,520
mimiciii |      prescriptions | 4,156,310
mimiciii |      procedureevents_mv |    258,066
mimiciii |      procedures_icd |        240,095
mimiciii |      services |      73,343
mimiciii |      transfers |     261,897

