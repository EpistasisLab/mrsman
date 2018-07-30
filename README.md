# mrsman
Medical Record System Management 

# about 
These tools load medical records from a mysql or postgres database into OpenMRS
and from OpenMRS into neo4j

# requirements
- postgres server 9.5+ (mimic source)
- mysql server 5.7+ (openmrs destination)
- neo4j 3.3.5 (with apoc 3.3 plugin)
- node 8+
- jdk1.6+
# optional
- docker 

# Loading MIMIC dataset into OpenMRS
1. Install [modified openmrs fhir module](https://github.com/djfunksalot/openmrs-module-fhir)  (_supports adding observation with link to encounter_)

2. Configure OpenMRS
- Start with an uninitialized OpenMRS system (do not log in).  OpenMRS is dependant on on "concept" records for everything from admissions to observations.  The simplest way to generate these them is by generating metadata on source data then inserting into the database.   Concepts must exist in the OpenMRS database upon system initialization which occurs during first login as the "admin" user.


3. Load mimic dataset into postgres using [modified mimic-code tools](https://github.com/djfunksalot/mimic-code) (_Creates additional indexes_)

4. Initialize source and destination databases
```bash
./python/import.py initDb
This step:
```
- Create additional tables in mimic database to track openmrs issued uuids
- Generate metadata for OpenMRS concepts

5. Log into OpenMRS
- Navigate to Maitenance->Advanced Settings, and set "validation.disable" to "true"

6. Import records
```bash
./python/import.py initLocations
./python/import.py initPractitioners
./python/import.py initPatients
./python/import.py initAdmit
```
 - Import steps must not be interupted.  Database changes are committed at completion of run.  Import limits may be imposed at each stage so that the system can iteratively import records.


# Loading OpenMRS data into neo4j
1. configure javascript environment
```bash
npm install
cd js/
cp config_example.js config.js  # edit this file for your environment
cp neo_example.json neo.json  # edit this file for your environment
```
2. start graphdb server
```bash
./neo.js rebuild -sv
```
3. log into neo4j and set the password as defined in config.json
4. generate the graph
```bash
./gengraph.js
```


# TODO
1. Load data from chartevents table
2. Create diagnoses, cause-of-death and transfer observations
3. associate caregivers with encounters?
4. Define notevent concepts
5. Create cause-of-death observations
6. Update fhir module to set cause of death to 'UNKNOWN' when adding deceased patients (so we don't need to disable validation)
