# mrsman
Medical Record System Management 

# about 
These tools load medical records from one system to another

# requirements
mysql-server 5.7+
node & npm
jdk1.6+
# optional
docker 


# loading demo data
- Start with uninitialized OpenMRS system  (do not log in)

## emrbots source records
- download 100,000-patient artificial EMR database from emrbots (http://www.emrbots.org/)
- load records into empty db by running sequential import scripts
```bash
cd sql/ 
mysql <dbname> < 01_create.sql  
mysql <dbname> < 02_load.sql
mysql <dbname> < 03_update.sql
```
## configure javascript environment
```bash
cd js/
npm install
cp config_example.js config.js  # edit this file for your environment
cp neo_example.json neo.json  # edit this file for your environment
```

## Running
```bash
node import.js
```


### MIMIC source data
1. Load mimic dataset using modified mimic-code tools:
- https://github.com/djfunksalot/mimic-code
_Creates additional indexes_


- Install modified OpenMRS fhir plugin:
-- https://github.com/djfunksalot/openmrs-module-fhir 
_supports adding observation with link to encounter_

- initialize database
_Create additional tables in mimic database to track openmrs issued uuids_
_Generate metadata for OpenMRS concepts_
```bash
./import.py initDb
```

- Initialize System
_Generate Records, order is important.  initPatients and initAdmit may be limited_
  ../import.py (initLocations, initPractitioners, initPatients, initAdmit)




#Import data into neo4j
_depends on apoc-3.3.0.2-all.jar neo4j plugin_
## (optional) run graphdb with required plugin in docker
## Running
```bash
cd <project_root>;node neo.js rebuild -sv
```

## Import OpenMRS data into neo4j
```bash
cd <project_root>/js;node gengraph.js
```
