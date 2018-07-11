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
-Start with an uninitialized OpenMRS system (do not log in).  OpenMRS is dependant on database resident concepts for everything from admissions to observations.  The simplest way to generate these concepts is by generating metadata on source data.   Concepts must exist in the OpenMRS database upon system initialization.  This occurs during first login as the "admin" user.
- Navigate to Maitenance->Advanced Settings, and set "validation.disable" to "true"


3. Load mimic dataset into postgres using [modified mimic-code tools] (https://github.com/djfunksalot/mimic-code) (_Creates additional indexes_)

4. Initialize source and destination databases
- Create additional tables in mimic database to track openmrs issued uuids
- Generate metadata for OpenMRS concepts
```bash
./import.py initDb
```
5. Log into OpenMRS

6. Import records
```bash
./import.py initLocations
./import.py initPractitioners
./import.py initPatients
./import.py initAdmit
```


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
