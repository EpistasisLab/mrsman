# mrsman
Medical Record System Management 
# about 
These tools load medical records from a mysql or postgres database into OpenMRS
and from OpenMRS into neo4j
# requirements
- postgres server 9.5+ (mimic source)
- mysql server 5.7+ (openmrs destination)
- node 8+
- jdk1.6+
- neo4j 3.3.5 (with apoc 3.3 plugin) || docker
# MIMIC-III
[loading the MIMIC-III dataset into OpenMRS](../master/mimic/LOADING.md)
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
