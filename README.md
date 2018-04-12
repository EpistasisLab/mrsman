# mrsman
Medical Record System Management

## requirements
mysql-server 5.7+
node & npm
jdk1.6+

## installation
### preparing source records
- download 100,000-patient artificial EMR database from [emrbots] (http://www.emrbots.org/)
- load records into empty db by running sequential import scripts
```bash
cd sql/ 
mysql <dbname> < 01_create.sql  
mysql <dbname> < 02_load.sql
mysql <dbname> < 03_update.sql
```

### configure javascript environment
```bash
cd js/
npm install
cp config_example.js config.js  # edit this file for your environment
```
## Running
```bash
node import.js
```
