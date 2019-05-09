exports.mysql_user = "mimic";
exports.mysql_pass = "mimic";
exports.mysql_host= "127.0.0.1";
exports.mysql_dbname= "odie";
exports.pg_user = "mimic";
exports.pg_pass = "mimic";
exports.pg_host= "127.0.0.1";
exports.pg_dbname= "mimic";

exports.url='http://sullust:5080/openmrs/ws';
exports.auth = "Basic " + new Buffer('admin:asVNTHdG1haUrNWb').toString("base64");
exports.dirs = { 
                  'Patient':'../data/Patient/',
                  'Concept':'../data/Concept/',
                  'Observation':'../data/Observation/',
                  'cypher':'../data/cypher/'
               };
exports.files = {
                  'Patient':'../data/PatientCorePopulatedTable.txt',
                  'Admission':'../data/AdmissionsCorePopulatedTable.txt',
                  'Observation':'../data/LabsCorePopulatedTable.txt',
                  'Concept':'../tmp/Concepts.txt',
                  'DemoPatients':'../data/DemoPatients_short.json',
                  'Encounter':'../tmp/Encounters.txt',
                  'Lab':'../tmp/Labs.txt',
                }
exports.json_files = {
                  'DemoPatients':'../data/DemoPatients_short.json',
}
exports.neo_user = "neo4j";
exports.neo_pass = "password";
exports.neo_uri = "bolt://127.0.0.1";
