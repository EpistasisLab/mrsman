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
| schema |      tablename|      num_records | resource
| ------------- |:-------------:| -----:| -------------:|
meta |  concepts |      44,615 | [Coding](https://www.hl7.org/fhir/datatypes.html#Coding)
meta |  encountertypes |        4 | [CodeableConcept](https://www.hl7.org/fhir/datatypes.html#CodeableConcept)
meta |  locations |     54 | [Location](https://www.hl7.org/fhir/location.html)
meta |  visittypes |    4 | [CodeableConcept](https://www.hl7.org/fhir/datatypes.html#CodeableConcept)
mimiciii |      [Admissions](https://mimic.physionet.org/mimictables/admissions/) |    58,976 | [Encounter](https://www.hl7.org/fhir/encounter.html)
mimiciii |      callout |  34,499  | [Encounter](https://www.hl7.org/fhir/encounter.html)
mimiciii |      caregivers |    7,567 | [Practitioner](https://www.hl7.org/fhir/practitioner.html)
mimiciii |      [chartevents](https://mimic.physionet.org/mimictables/chartevents/) |   330,714,033 | [Observation](https://www.hl7.org/fhir/observation.html)
mimiciii |      [cptevents](https://mimic.physionet.org/mimictables/cptevents/)  |     573,146 | [Procedure](https://www.hl7.org/fhir/procedure.html)
mimiciii |      [d_cpt](https://mimic.physionet.org/mimictables/d_cpt/) | 134 | [Coding](https://www.hl7.org/fhir/datatypes.html#Coding)
mimiciii |      [d_icd_diagnoses](https://mimic.physionet.org/mimictables/d_icd_diagnoses/) |       14,567 | [Coding](https://www.hl7.org/fhir/datatypes.html#Coding)
mimiciii |      [d_icd_procedures](https://mimic.physionet.org/mimictables/d_icd_procedures/) | 3,882 | [Coding](https://www.hl7.org/fhir/datatypes.html#Coding)
mimiciii |      [d_items](https://mimic.physionet.org/mimictables/d_items/) |       12,487 | [Coding](https://www.hl7.org/fhir/datatypes.html#Coding)
mimiciii |      [d_labitems](https://mimic.physionet.org/mimictables/d_labitems/) |    753 | [Coding](https://www.hl7.org/fhir/datatypes.html#Coding)
mimiciii |      [datetimeevents](https://mimic.physionet.org/mimictables/datetimeevents) |        4,486,425 | [Observation](https://www.hl7.org/fhir/observation.html)
mimiciii |      [diagnoses_icd](https://mimic.physionet.org/mimictables/diagnoses_icd/) | 651,047 | [Condition](https://www.hl7.org/fhir/condition.html) 
mimiciii |      [drgcodes](https://mimic.physionet.org/mimictables/drgcodes/) |      125,557 | [Condition](https://www.hl7.org/fhir/condition.html)
mimiciii |      [icustays](https://mimic.physionet.org/mimictables/icustays/) |      61,532 |  [Encounter](https://www.hl7.org/fhir/encounter.html)
mimiciii |      [inputevents_cv](https://mimic.physionet.org/mimictables/inputevents_cv/) |        17,527,936 | [Observation](https://www.hl7.org/fhir/observation.html)
mimiciii |      [inputevents_mv](https://mimic.physionet.org/mimictables/inputevents_mv/) |        3,619,423 | [Observation](https://www.hl7.org/fhir/observation.html)
mimiciii |      [labevents](https://mimic.physionet.org/mimictables/labevents/) |     27,860,624 | [Observation](https://www.hl7.org/fhir/observation.html)
mimiciii |      [microbiologyevents](https://mimic.physionet.org/mimictables/microbiologyevents/) |    631,726
mimiciii |      [noteevents](https://mimic.physionet.org/mimictables/microbiologyevents/) |    2,078,216
mimiciii |      [outputevents](https://mimic.physionet.org/mimictables/outputevents) |  4,349,263 | [Observation](https://www.hl7.org/fhir/observation.html)
mimiciii |      [patients](https://mimic.physionet.org/mimictables/patients) |      46,520 | [Patient](https://www.hl7.org/fhir/patient.html)
mimiciii |      [prescriptions](https://mimic.physionet.org/mimictables/prescriptions) | 4,156,310 | [MedicationRequest](https://www.hl7.org/fhir/medicationrequest.html)
mimiciii |      [procedureevents_mv](https://mimic.physionet.org/mimictables/procedureevents_mv) |    258,066 | [Procedure](https://www.hl7.org/fhir/procedure.html)
mimiciii |      [procedures_icd](https://mimic.physionet.org/mimictables/procedures_icd) |        240,095 | [Procedure](https://www.hl7.org/fhir/procedure.html)
mimiciii |      [services](https://mimic.physionet.org/mimictables/services) |      73,343 | [Encounter](https://www.hl7.org/fhir/encounter.html)
mimiciii |      [transfers](https://mimic.physionet.org/mimictables/transfers) |     261,897 | [Encounter](https://www.hl7.org/fhir/encounter.html)

