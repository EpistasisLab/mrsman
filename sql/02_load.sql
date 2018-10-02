LOAD DATA LOCAL INFILE 'AdmissionsCorePopulatedTable.txt' INTO TABLE Admissions IGNORE 1 LINES;
LOAD DATA LOCAL INFILE 'AdmissionsDiagnosesCorePopulatedTable.txt'  INTO TABLE AdmissionsDiagnoses IGNORE 1 LINES;
LOAD DATA LOCAL INFILE 'LabsCorePopulatedTable.txt' INTO TABLE Labs IGNORE 1 LINES;
LOAD DATA LOCAL INFILE 'PatientCorePopulatedTable.txt' INTO TABLE Patients IGNORE 1 LINES;
