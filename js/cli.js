#!/usr/bin/env node
 // import patients from example dataset provided by:
// "A 100,000-patient database that contains in total 100,000 patients, 361,760 admissions, and 107,535,387 lab observations."
// http://arxiv.org/pdf/1608.00570.pdf 

// basic requires
const http = require('http');
const Q = require("q"); // v2.11.1
const randomname = require('node-random-name')
const rp = require('request-promise');
//ui
const blessed = require('blessed');
const bcontrib = require('blessed-contrib');
const fs = require('fs')
const transform = require('stream-transform');
const csv = require("fast-csv");
const uuidv4 = require('uuid/v4');
const mysql = require('mysql');
const vorpal = require('vorpal')();
const util = require('util')
var Concept = require('./model/Concept');

// configs
var config = require('./config');

//openmrs specific checkstring for patient ids
const checkstring = '0123456789ACDEFGHJKLMNPRTUVWXY'

//number of cuncurrent http or database requests 
var stepsize = 100;

//number of records proccessed
var count = 0;

//number of failed requests
var failcount = 0;

//number of successful requests
var successcount = 0;

//array for working data
var output = {};
output['fhir'] = {}
output['rest'] = {}
output['raw'] = {}


// Define the connection to MySQL database.
var pool = mysql.createPool({
    host: config.mysql_host,
    user: config.mysql_user,
    password: config.mysql_pass,
    database: config.mysql_dbname,
    connectionLimit: 20, // Default value is 10.
    waitForConnections: true, // Default value.
    queueLimit: 0 // Unlimited - default value.
});

//helper function for db queries
var db = {
    query: function(sql, params) {
        var deferred = Q.defer();
        pool.query(sql, params, deferred.makeNodeResolver());
        return (deferred.promise);
    }
};

//generate mysql timestamp
Date.prototype.toMysqlFormat = function() {
    function twoDigits(d) {
        if (0 <= d && d < 10) return "0" + d.toString();
        if (-10 < d && d < 0) return "-0" + (-1 * d).toString();
        return d.toString();
    }
    return this.getUTCFullYear() + "-" + twoDigits(1 + this.getUTCMonth()) + "-" + twoDigits(this.getUTCDate()) + " " + twoDigits(this.getHours()) + ":" + twoDigits(this.getUTCMinutes()) + ":" + twoDigits(this.getUTCSeconds());
};



//convert record to fhir syntax based on resource type
//var resourceTrans = function(record, resource) {
//    var transformer = {}
//    transformer['Observation'] = observationTransformer;
//    return transformer[resource](record);
//}

//convert record to fhir syntax based on resource type
var transformRecord = function(record, resource) {
    if (resource == 'Patient') {
        var transformer = patientTransformer;
    } else if (resource == 'Encounter') {
        var transformer = encounterTransformer;
    } else if (resource == 'Observation') {
        var transformer = observationTransformer;
    } else if (resource == 'Concept') {
        var transformer = conceptTransformer;
    }
    return transformer(record);
}


var initResources = function() {
    var deferred = Q.defer();


    var visit_type = {
        date_created: new Date().toMysqlFormat(),
        visit_type_id: 1,
        creator: 1,
        uuid: '30e23a41-7723-41f8-ac57-db22f01ebbba',
        name: "inpatient",
        description: "Inpatient Visit"
    }
    output['raw']['visit_type'] = [visit_type];

    var practitioner = {
        "resourceType": "Practitioner",
        "id": "9a1beb05-35da-47ef-8155-90d1e3cec4e7",
        "name": {
            "family": [
                "Squash"
            ],
            "given": [
                "Joshua"
            ]
        },
        address: [{
            use: "home",
            city: "E. Kanateng"
        }],
        gender: "male",
        birthDate: "2010-04-03T00:00:00"
    }
    output['fhir']['Practitioner'] = [practitioner]


    return Q.allSettled([readResource('Concept', 'LabName'), readResource('Visit', 'patient_visit'), readResource('Lab')]);

}


function readResource(resource, index) {
    var deferred = Q.defer();
    output['raw'][resource] = [];
    var filename = config.files[resource];

    if (fs.existsSync(filename)) {
        var stream = fs.createReadStream(filename);
        csv
            .fromStream(stream, {
                headers: true,
                delimiter: '\t'
            })
            .on("data", function(data) {
                if (index) {
                    output['raw'][resource][data[index]] = data;
                } else {
                    output['raw'][resource].push(data);
                }
            })
            .on("end", function() {
                console.log("reading " + filename)
                deferred.resolve();
            });
    } else {
        deferred.reject();
    }
    return deferred.promise;


}




// create fhir compatible patient object
var patientTransformer = function(record) {
    //console.log(record);
    function luhnCheckDigit(number) {
        var validChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVYWXZ_";
        var sum = 0;
        number = number.toString();
        for (var i = 0; i < number.length; i++) {
            var ch = number.charAt(number.length - i - 1);
            if (validChars.indexOf(ch) < 0) {
                console.log("Invalid character(s) found!");
                return false;
            }
            var digit = ch.charCodeAt(0) - 48;
            var weight;
            if (i % 2 == 0) {
                weight = (2 * digit) - parseInt(digit / 5) * 9;
            } else {
                weight = digit;
            }
            sum += weight;
        }
        sum = Math.abs(sum) + 10;
        var digit = (10 - (sum % 10)) % 10;
        return digit;
    }
    var uuid = record.PatientID.toLowerCase();
    var num = (new Date).getTime().toString().substring(8, 13) + record.num;
    var checkdigit = luhnCheckDigit(num);
    var OpenMRSID = num + '-' + checkdigit;
    var gender = record.PatientGender.toLowerCase();
    var identifier = record.PatientID.toLowerCase()
    var birthdate = new Date(record.PatientDateOfBirth).toISOString().substr(0, 10);
    var givenName = randomname({
        random: Math.random,
        first: true,
        gender: gender
    })
    var familyName = randomname({
        random: Math.random,
        last: true
    })

    var patient = {
        "resourceType": "Patient",
        "id": uuid,
        "identifier": [{
            "use": "usual",
            "system": "OpenMRS Identification Number",
            "value": OpenMRSID
        }],
        "name": [{
            "use": "usual",
            "family": familyName,
            "given": [
                givenName
            ]
        }],
        "gender": gender,
        "birthDate": birthdate,
        "deceasedBoolean": false,
        "active": true
    }
    return patient;
}

//openmrs specific format for lab values from concept record
conceptTransformer = function(record) {
    var description = record.name + " in " + record.system + " assay";
    var long_name = record.LabName;
    var short_name = record.name
    var hi_normal = record.max;
    var low_normal = record.min;
    var hi_absolute = record.max;
    var low_absolute = record.min;
    var hi_critical = record.max;
    var low_critical = record.min;
    var date = new Date().toMysqlFormat(); //return MySQL Datetime format
    var daughters = [];
    var concept = {
        "datatype_id": "1",
        "date_created": date,
        "class_id": "1",
        "creator": "1",
        "uuid": record.uuid
    };
    daughters.push({
        "concept_description": {
            "concept_id": null,
            "date_created": date,
            "date_changed": date,
            "locale": "en",
            "creator": "1",
            "changed_by": "1",
            "description": description,
            "uuid": uuidv4()
        }
    });
    daughters.push({
        "concept_name": {
            "concept_id": null,
            "name": long_name,
            "date_created": date,
            "creator": "1",
            "locale": "en",
            "locale_preferred": "1",
            "concept_name_type": "FULLY_SPECIFIED",
            "uuid": uuidv4()
        }
    });
    daughters.push({
        "concept_name": {
            "concept_id": null,
            "name": short_name,
            "date_created": date,
            "creator": "1",
            "locale": "en",
            "locale_preferred": "0",
            "concept_name_type": "SHORT",
            "uuid": uuidv4()
        }
    });
    daughters.push({
        "concept_numeric": {
            "concept_id": null,
            "units": record.units,
            "hi_normal": hi_normal,
            "low_normal": low_normal,
            "hi_absolute": hi_absolute,
            "low_absolute": low_absolute,
            "hi_critical": hi_critical,
            "low_critical": low_critical
        }
    });
    return {
        concept,
        daughters
    };
}


//placeholder
var encounterTransformer = function(record) {
    var date_start = new Date(record.AdmissionStartDate).toISOString();
    var date_end = new Date(record.AdmissionEndDate).toISOString();
    var patient_uuid = record.PatientID.toLowerCase();
    var visit = {
        "resourceType": "Encounter",
        "type": [{
            "coding": [{
                "code": "1"
            }]
        }],
        "subject": {
            "id": patient_uuid,
        },
        "participant": [{
            "individual": {
                "reference": "Practitioner/9a1beb05-35da-47ef-8155-90d1e3cec4e7",
                "display": "Adam Careful(Identifier:null)"
            }
        }],
        "period": {
            "start": date_start,
            "end": date_end
        },
        "location": [{
            "location": {
                "reference": "Location/8d6c993e-c2cc-11de-8d13-0010c6dffd0f",
                "display": "Unknown Location"
            },
            "period": {
                "start": date_start,
                "end": date_end
            }
        }]
    }
    return visit;
}

//placeholder
observationTransformer = function(record) {
    var date = new Date(record.LabDateTime).toISOString();
    var patient_uuid = record.PatientID.toLowerCase();
    var concept_uuid;
    for (i in output['raw']['Concept']) {
        var concept = output['raw']['Concept'][i];
        if (concept['LabName'] == record.LabName) {
            concept_uuid = concept['uuid'];
        }

    }

    var observation = {
        "resourceType": "Observation",
        "code": {
            "coding": [{
                "system": "http://openmrs.org",
                "code": concept_uuid
            }]
        },
        "subject": {
            "id": patient_uuid,
        },
        "effectiveDateTime": date,
        "issued": date,
        "valueQuantity": {
            "value": record.LabValue,
            "unit": record.LabUnits,
            "system": "http://unitsofmeasure.org",
        }
    }
    return observation;
}

//process text files for import
var importRawResource = function(resource) {
    var deferred = Q.defer();
    var promises = output['raw'][resource].map(
        function iterator(record) {
            console.log('record');
            console.log(record);
            var sql = "INSERT INTO " + resource + " (" + Object.keys(record).join(",") + ") values (\"" + Object.values(record).join("\",\"") + "\")";
            console.log(sql);
            var dbpromise = db
                .query(sql)
                .then(
                    function handleResponse(results) {
                        return results;
                    },
                    function handleError(error) {
                        if (error.sqlMessage !== undefined) {
                            console.log(error.sqlMessage);
                        }
                        return 0
                    }
                )
                .catch(function(err) {
                    console.log(err);
                    return 0
                });
            return dbpromise;
        })

    Q.allSettled(promises)
        .then(
            function handleSettled(sn2) {
                pool.end();
                return deferred.resolve();
            }
        )
        .catch(function(err) {
            pool.end();
            return deferred.reject();
        });
    return deferred.promise;
}



//insert concepts into sql
var importConcepts = function() {
    var resource = 'Concept';
    var deferred = Q.defer();
    var filename = config.files[resource]
    var stream = fs.createReadStream(filename);
    // var updated = [];
    var concepts = [];
    //perform database inserts
    function request() {

        //create daughter records
        function decorateRecords(records) {
            var promises = Array(records.length);
            for (var l in records) {
                var r = records[l]
                for (m in r) {
                    var keys = Object.keys(r[m])
                    var vals = Object.values(r[m])
                    var tab = m;
                    var sql = "INSERT INTO " + tab + " (" + keys.join(",") + ") values (\"" + vals.join("\",\"") + "\")";
                    //console.log(sql);
                    promises[l] = db
                        .query(sql)
                        .then(
                            function handleResponse(results) {
                                return 1;
                            },
                            function handleError(error) {
                                console.log("error inserting daughter records");
                                console.log(error);
                                return 0
                            }
                        );
                }
            }
            return promises;
        }

        //create parent records
        var promises = concepts.map(
            function iterator(r) {
                // console.log(r);
                var concept = r.concept;
                var daughters = r.daughters;
                var sql = "INSERT INTO concept (" + Object.keys(concept).join(",") + ") values (\"" + Object.values(concept).join("\",\"") + "\")";
                var promise = db
                    .query(sql)
                    .then(
                        function handleResponse(results) {
                            var insertid = results[0].insertId;
                            for (n in daughters) {
                                var daughter = daughters[n];
                                //console.log(daughter);
                                Object.keys(daughter).forEach(function(key) {
                                    var val = daughter[key];
                                    Object.keys(val).forEach(function(k) {
                                        if (k == "concept_id") {
                                            val[k] = insertid;
                                        }
                                    });
                                    daughter[key] = val;
                                });
                                daughters[n] = daughter;
                            }
                            //set id for dependant records
                            //delete r.concept;
                            return true;
                        },
                        function handleError(error) {
                            if (error.sqlMessage !== undefined) {
                                console.log(error.sqlMessage);
                            }
                            return 0
                        }
                    );
                return promise;
            })
        Q.allSettled(promises)
            .then(
                function handleSettled(sn2) {
                    var records = [];
                    for (i in sn2) {
                        var response = sn2[i];
                        if (response.state == 'fulfilled') {
                            //                    console.log(sn2[i]);
                            for (j in response.value) {
                                records.push(response.value[j]);
                            }
                        }
                    }

                    var decorate = decorateRecords(records);
                    Q.allSettled(decorate)
                        .then(
                            function handleSettled(sn2) {
                                return deferred.resolve();
                                pool.end();
                            }
                        );
                });

    }





    csv
        .fromStream(stream, {
            headers: true,
            delimiter: '\t'
        })
        .transform(function(obj) {
            var transformed = transformRecord(obj, resource);
            return transformed;
        })
        .on("data", function(data) {
            concepts.push(data);
        })
        .on("end", function() {
            console.log("Running raw requests")
            request(resource)
        });
    return deferred.promise;
}

//load FHIR data into local array
var getResource = function(cat, resource) {
    //   var updated = []
    count = 0, failcount = 0, successcount = 0
    var deferred = Q.defer();
    if (cat == 'fhir') {
        var baseuri = config.url + '/fhir/' + resource
    } else {
        var baseuri = config.url + '/rest/v1/' + resource.toLowerCase();
    }

    function request(resource, count) {
        var handleRecord = function(record) {
                var method = 'GET';
                var uri = baseuri;
            var options = {
                method: method,
                uri: uri,
                resolveWithFullResponse: true,
                headers: {
                    "Authorization": config.auth
                },
                json: record // Automatically stringifies the body to JSON
            };
            var promise = rp(options)
            return (promise);
        }

        var promises = new Array(stepsize);
        if (count > output[cat][resource].length) {
            deferred.resolve();
            //            output[cat][resource] = updated;
            return
        } else if (count == undefined) {
            count = 0
        }
        for (var i = 0; i < stepsize; i++) {
            var record = output[cat][resource][count + i];
            if (record) {
                promises[i] = handleRecord(record);
            }
        }
        Q.allSettled(promises)
            .then(function(results) {
                for (var b in results) {
                    //var record_num = (Number(b) + Number(count).toString();
                    //console.log(Number(b) + Number(count));
                    var result = results[b];
                    if (result.state === "fulfilled") {
                        console.log(result.value.request.body);
                        successcount++
                    } else {
                        failcount++
                    }
                }

                console.log({
                    count,
                    stepsize
                });
                console.log({
                    successcount,
                    failcount
                });
                request(resource, count + stepsize);
                var percent_success = successcount / output[cat][resource].length;
            })
            .catch(function(err) {
                console.log(err);
                request(resource, count + stepsize);
            });
    }
    return deferred.promise;
}

//process text files for FHIR import
var postResource = function(cat, resource) {
    //   var updated = []
    count = 0, failcount = 0, successcount = 0
    var deferred = Q.defer();
    if (cat == 'fhir') {
        var baseuri = config.url + '/fhir/' + resource
    } else {
        var baseuri = config.url + '/rest/v1/' + resource.toLowerCase();
    }

    function request(resource, count) {
        var handleRecord = function(record) {


            if (record.id !== undefined) {
                var method = 'PUT';
                var uri = baseuri + '/' + record.id;
            } else {
                var method = 'POST';
                var uri = baseuri;
            }
            var options = {
                method: method,
                uri: uri,
                resolveWithFullResponse: true,
                headers: {
                    "Authorization": config.auth
                },
                json: record // Automatically stringifies the body to JSON
            };
            var promise = rp(options)
            return (promise);
        }

        var promises = new Array(stepsize);
        if (count > output[cat][resource].length) {
            deferred.resolve();
            //            output[cat][resource] = updated;
            return
        } else if (count == undefined) {
            count = 0
        }
        for (var i = 0; i < stepsize; i++) {
            var record = output[cat][resource][count + i];
            if (record) {
                promises[i] = handleRecord(record);
            }
        }
        Q.allSettled(promises)
            .then(function(results) {
                for (var b in results) {
                    //var record_num = (Number(b) + Number(count).toString();
                    //console.log(Number(b) + Number(count));
                    var result = results[b];
                    if (result.state === "fulfilled") {
                        console.log(result.value.request.body);
                        //                        var loc = result.value.headers['location']
                        //console.log(result.value);
                        //                        var uuid = loc.split("\/").pop()
                        //                        var rec = JSON.parse(result.value.request.body);
                        //                        rec['id'] = uuid;
                        //                        updated.push(rec);
                        successcount++
                    } else {
                        failcount++
                    }
                }

                console.log({
                    count,
                    stepsize
                });
                console.log({
                    successcount,
                    failcount
                });
                request(resource, count + stepsize);
                var percent_success = successcount / output[cat][resource].length;
            })
            .catch(function(err) {
                console.log(err);
                request(resource, count + stepsize);
            });
    }

    if (config.files[resource] !== undefined && output[cat][resource] === undefined) {
        output[cat][resource] = []
        var filename = config.files[resource]
        var stream = fs.createReadStream(filename);
        var num = 0;
        csv
            .fromStream(stream, {
                headers: true,
                delimiter: '\t'
            })
            .transform(function(obj) {
                num++;
                obj.num = num;
                var transformed = transformRecord(obj, resource);
                return transformed;
            })
            .validate(function(data) {
                return data.resourceType !== undefined; //resources need type
            })
            .on("data-invalid", function(data) {
                console.log("invalid data")
            })
            .on("data", function(data) {
                output[cat][resource].push(data);
            })
            .on("end", function() {
                console.log("done");
                console.log("Running sequential requests!")
                request(resource);
            });
    } else {
        request(resource);
    }
    return deferred.promise;
}

var formatRecords = function(resource, records, data) {
    if (resource == 'Visit') {
        var patient_visit = data['PatientID'] + '-v' + data['AdmissionID'];
        data['patient_visit'] = patient_visit;
        data['id'] = uuidv4();
        records[patient_visit] = data;
    }
    if (resource == 'Concept') {


        if (records[data.LabName] === undefined) {
            var assay = data.LabName.split(': ');
            var name = assay[1].toLowerCase();
            var system = assay[0].toLowerCase();
            var record = {
                'LabName': data.LabName,
                'name': name,
                'system': system,
                'units': data.LabUnits,
                'min': data.LabValue,
                'max': data.LabValue,
                'uuid': uuidv4(),
            }
            records[data.LabName] = record;
        } else if (Number(records[data.LabName]['min']) > Number(data['LabValue'])) {
            records[data.LabName]['min'] = data['LabValue'];
        } else if (Number(records[data.LabName]['max']) < Number(data['LabValue'])) {
            records[data.LabName]['max'] = data['LabValue'];
        }
    }



    return records;
}

//generate a new resource based on objects in an existing one
//save resource to a file and output.raw array
var genResource = function(source, destination) {
    var readfile = config.files[source]
    var writefile = config.files[destination]
    var readstream = fs.createReadStream(readfile);
    var writestream = fs.createWriteStream(writefile);
    var csvStream = csv.createWriteStream({
        headers: true,
        delimiter: '\t'
    });
    var records = []
    var total = 0;
    var deferred = Q.defer();
    console.log("reading" + readfile)
    csv
        .fromStream(readstream, {
            headers: true,
            delimiter: '\t'
        })
        //    .validate(function(data) {
        //        return data.LabName !== undefined; //todo
        //    })
        .on("data-invalid", function(data) {
            console.log("invalid data")
        })
        .on("data", function(data) {

            records = formatRecords(destination, records, data);
            total++;
            if (total % 100000 === 0) {
                console.log("processed " + total + " records");
            }

        })
        .on("end", function() {
            console.log("done reading " + readfile);
            console.log("start writing " + writefile)
            csvStream.pipe(writestream);
            for (record in records) {
                csvStream.write(records[record]);
            }
            output['raw'][destination] = records;
            csvStream.end();
            writestream.on("finish", function() {
                console.log("done writing " + writefile);
                deferred.resolve();
            });
        });
    return deferred.promise;
}





//output data dimensions
var summa = function() {
    Object.keys(output).map(function(cat) {
        console.log(cat);
        Object.keys(output[cat]).map(function(model) {
            console.log(' - ' + model);
        });

    });
}


//ui
vorpal
    .command('genconcepts', 'Generate Lab Dictionary')
    .action(function(args, callback) {
        this.log('generating concepts');
        genResource('Observation', 'Concept')
            .done(function() {
                summa();
                callback();
            });
    });


vorpal
    .command('importconcepts', 'Import Dictionary')
    .action(function(args, callback) {
        this.log('trying sql import');
        importConcepts()
            .done(function() {
                summa();
                callback();
            });
    });

vorpal
    .command('genencounters', 'Generate Visits')
    .action(function(args, callback) {
        this.log('trying sql import');
        genResource('Admission', 'Encounter')
            .done(function() {
                summa();
                callback();
            });
    });


vorpal
    .command('importvisittypes', 'Import Visit Types')
    .action(function(args, callback) {
        this.log('trying rest import of visit types');
        importRawResource('visit_type')
            .done(function() {
                summa();
                callback();
            });
    });

vorpal
    .command('loadmd', 'Import Practitioners')
    .action(function(args, callback) {
        this.log('trying fhir import of practitioners');
        postResource('fhir', 'Practitioner')
            .done(function() {
                summa();
                callback();
            });
    });


vorpal
    .command('loadpatients', 'Import Patients')
    .action(function(args, callback) {
        this.log('trying fhir import of patients');
        postResource('fhir', 'Patient')
            .done(function() {
                summa();
                callback();
            });
    });

vorpal
    .command('loadencounters', 'Import Visits')
    .action(function(args, callback) {
        this.log('trying fhir import of encounters');
        postResource('fhir', 'Encounter')
            .done(function() {
                summa();
                callback();
            });
    });


vorpal
    .command('loadlabs', 'Import Observations')
    .action(function(args, callback) {
        this.log('trying fhir import of observations');
        postResource('fhir', 'Observation')
            .done(function() {
                summa();
                callback();
            });
    });

vorpal
    .command('getpatients', 'Download Patients')
    .action(function(args, callback) {
        this.log('trying to download patients');
        getResource('rest', 'Patient')
            .done(function() {
                summa();
                callback();
            });
    });




vorpal
    .command('inspect [cat] [resource]', 'Print the specified resource')
    .action(function(args, callback) {
        if (args.cat !== undefined && args.resource !== undefined) {
            this.log('inspecting data in output[' + args.cat + ']' + '[' + args.resource + ']');
            console.log(output[args.cat][args.resource]);
        } else {
            summa();
        }
        callback();
    });

initResources().done(function() {
    if (process.argv.length >= 3) {
        vorpal.exec(process.argv[2]).then(function(data) {
            console.log('fin');
        })
    } else {
        summa();
        vorpal.delimiter('mrsman$').show();
    }
});
