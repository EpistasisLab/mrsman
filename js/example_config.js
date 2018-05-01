exports.mysql_user = "root";
exports.mysql_pass = "<password>";
exports.mysql_host= "127.0.0.1";
exports.mysql_dbname= "openmrs";
exports.url='http://localhost:8080/openmrs/ws';
exports.auth = "Basic " + new Buffer('admin:Admin123').toString("base64");
exports.files = {
                  'Patient':'../data/PatientCorePopulatedTable_short.txt',
                  'Encounter':'../data/AdmissionsCorePopulatedTable_short.txt',
                  'Observation':'../data/LabsCorePopulatedTable_short.txt',
                  'Concept':'../tmp/Concepts.txt',
                }
exports.neo4j_user = "neo4j";
exports.neo4j_pass = "<password>";
