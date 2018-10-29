# mrsman
Medical Record System Management 
# Loading MIMIC dataset into OpenMRS
1. Configure OpenMRS
- Start with an uninitialized OpenMRS system (do not log in)
3. Load mimic dataset into postgres by following [these instructions](https://github.com/EpistasisLab/mimic-code/tree/master/buildmimic/postgres)
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
```
