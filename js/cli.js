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
var resourceTrans = function(record, resource) {
    var transformer = {}
    transformer['Observation'] = observationTransformer;
    return transformer[resource](record);
}

//convert record to fhir syntax based on resource type
var transformRecord = function(record, resource) {
    if (resource == 'Patient') {
        var formatter = patientFormatter;
    } else if (resource == 'Encounter') {
        var formatter = encounterFormatter;
    } else if (resource == 'Observation') {
        var formatter = observationFormatter;
    } else if (resource == 'Concept') {
        var formatter = conceptFormatter;
    }
    return formatter(record);
}

var visittype = [{
    date_created: new Date().toMysqlFormat(),
    visit_type_id:1,
    creator:1,
    name:"inpatient",
    description:"Inpatient Visit"
}]
output['raw']['visittype'] = [visittype];

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

// create fhir compatible patient object
var patientFormatter = function(record) {
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
    var num = (new Date).getTime().toString().substring(8,13) + record.num;
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
conceptFormatter = function(record) {
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
var encounterFormatter = function(record) {
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
observationFormatter = function(record) {
    var date = new Date(record.LabDateTime).toISOString();
    var patient_uuid = record.PatientID.toLowerCase();
    var observation = {
        "resourceType": "Observation",
        "code": {
            "coding": [{
                "code": record.LabName
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

//remove fields we don't need to generate unique observation concepts
var observationTransformer = function(record) {
    delete record['PatientID'];
    delete record['AdmissionID'];
    delete record['LabDateTime'];
    return record;
}


//process text files for import
var importRawResource = function(resource) {
    var deferred = Q.defer();
    var filename = config.files[resource]
    var stream = fs.createReadStream(filename);
    output['raw'][resource] = []
    //perform database inserts
    function rawRequest(resource) {

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
        var promises = output['raw'][resource].map(
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
                            delete r.concept;
                            return daughters;
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
            output['raw'][resource].push(data);
        })
        .on("end", function() {
            console.log("Running raw requests")
            rawRequest(resource)
        });
    return deferred.promise;
}

//process text files for FHIR import
var postResource = function(cat,resource) {
    var deferred = Q.defer();
    var uri = config.url 
if(cat == 'fhir') {
    uri = uri + '/fhir/' + resource
} else {
    uri = uri + '/rest/v1/' + resource.toLowerCase();
}

    function request(resource, count) {
        //console.log('request');
        //console.log({count});
        var handleRecord = function(record) {


if(record.id !== undefined) {
    var method = 'PUT';
    uri = uri + '/' + record.id;
} else {
    var method = 'POST';
}
            //console.log(record);
            var options = {
                method: method,
                uri: uri,
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
                results.forEach(function(result) {
                    //console.log(JSON.stringify(result));
                    if (result.state === "fulfilled") {
                        successcount++
                        //var value = result.value;
                    } else {
                        //var reason = result.reason;
                        failcount++
                    }
                });
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

if(config.files[resource] !==undefined  &&  output[cat][resource]  === undefined) {
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

var summarize = function(source, destination) {
    var readfile = config.files[source]
    var writefile = config.files[destination]
    var readstream = fs.createReadStream(readfile);
    var writestream = fs.createWriteStream(writefile);
    var csvStream = csv.createWriteStream({
        headers: true,
        delimiter: '\t'
    });
    var concepts = {}
    var total = 0;
    var deferred = Q.defer();
    var formatter = function(input) {
        var assay = input['LabName'].split(': ');
        var name = assay[1].toLowerCase();
        var system = assay[0].toLowerCase();
        return {
            'LabName': input['LabName'],
            'name': name,
            'system': system,
            'units': input['LabUnits'],
            'min': input['min'],
            'max': input['max'],
            'uuid': uuidv4(),
        }
    }
    console.log("reading" + readfile)
    csv
        .fromStream(readstream, {
            headers: true,
            delimiter: '\t'
        })
        .transform(function(obj) {
            var transformed = resourceTrans(obj, source);
            return transformed;
        })
        .validate(function(data) {
            return data.LabName !== undefined; //todo
        })
        .on("data-invalid", function(data) {
            console.log("invalid data")
        })
        .on("data", function(data) {
            if (concepts[data.LabName] === undefined) {
                concepts[data.LabName] = data;
            } else if (concepts[data.LabName]['min'] === undefined) {
                concepts[data.LabName]['min'] = data['LabValue'];
            } else if (concepts[data.LabName]['max'] === undefined) {
                concepts[data.LabName]['max'] = data['LabValue'];
            } else if (concepts[data.LabName]['min'] > data['LabValue']) {
                concepts[data.LabName]['min'] = data['LabValue'];
            } else if (concepts[data.LabName]['max'] < data['LabValue']) {
                concepts[data.LabName]['max'] = data['LabValue'];
            }
            total++;
            if (total % 100000 === 0) {
                console.log("processed " + total);
            }
        })
        .on("end", function() {
            console.log("done reading " + readfile);
            console.log("start writing " + writefile)
            csvStream.pipe(writestream);
            for (LabName in concepts) {
                var concept = formatter(concepts[LabName])
                concepts[LabName] = concept;
                csvStream.write(concept);
            }
            output['raw'][destination] = concepts;
            csvStream.end();
            writestream.on("finish", function() {
                console.log("done writing " + writefile);
                deferred.resolve();
            });
        });
    return deferred.promise;
}


if (process.argv.length >= 3) {
    if (process.argv[2] == 'genconcepts') {
        summarize('Observation', 'Concept');
    }
    if (process.argv[2] == 'loadpatients') {
        postResource('fhir','Patient')
    }
    if (process.argv[2] == 'loadmd') {
        postResource('fhir','Practitioner')
    }

} else {

    var summa = function() {
        Object.keys(output).map(function(cat) {
            console.log(cat);
            Object.keys(output[cat]).map(function(model) {
                console.log(' - ' + model + '(' + model.length + ')');
            });

        });
        //  console.log(util.inspect(output[args.resource], {showHidden: false, depth: null}))
    }
    //ui
    vorpal
        .command('genconcepts', 'Generate Lab Dictionary')
        .action(function(args, callback) {
            this.log('generating concepts');
            summarize('Observation', 'Concept')
                .done(function() {
                    summa();
                    callback();
                });
        });

    vorpal
        .command('loadconcepts', 'Import Dictionary')
        .action(function(args, callback) {
            this.log('trying sql import');
            importRawResource('Concept')
                .done(function() {
                    summa();
                    callback();
                });
        });

    vorpal
        .command('loadmd', 'Import Practitioners')
        .action(function(args, callback) {
            this.log('trying fhir import of practitioners');
            postResource('fhir','Practitioner')
                .done(function() {
                    summa();
                    callback();
                });
        });

    vorpal
        .command('loadvisittypes', 'Import Visit Types')
        .action(function(args, callback) {
            this.log('trying rest import of visit types');
            importRawResource('visittype')
                .done(function() {
                    summa();
                    callback();
                });
        });

    vorpal
        .command('loadpatients', 'Import Patients')
        .action(function(args, callback) {
            this.log('trying fhir import of patients');
            postResource('fhir','Patient')
                .done(function() {
                    summa();
                    callback();
                });
        });

    vorpal
        .command('loadencounters', 'Import Visits')
        .action(function(args, callback) {
            this.log('trying fhir import of encounters');
            postResource('fhir','Encounter')
                .done(function() {
                    summa();
                    callback();
                });
        });


    vorpal
        .command('loadlabs', 'Import Observations')
        .action(function(args, callback) {
            this.log('trying fhir import of observations');
            postResource('fhir','Observation')
                .done(function() {
                    summa();
                    callback();
                });
        });



    vorpal
        .command('inspect [cat] [resource]', 'Print the specified resource')
        .action(function(args, callback) {
            this.log('inspecting data in output[' + args.cat + ']' + '[' + args.resource + ']');
            console.log(output[args.cat][args.resource]);
            //            console.log(util.inspect(output[args.resource], {showHidden: false, depth: null}))
            callback();
        });

    vorpal
        .delimiter('mrsman$')
        .show();
}
