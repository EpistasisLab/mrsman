#!/usr/bin/env node
 // import patients from example dataset provided by:
// "A 100,000-patient database that contains in total 100,000 patients, 361,760 admissions, and 107,535,387 lab observations."
// http://arxiv.org/pdf/1608.00570.pdf 

// basic requires
const http = require('http');
const Q = require("q"); // v2.11.1
const luhn = require('luhn-mod-n');
const randomname = require('node-random-name')
const rp = require('request-promise');
//ui
const blessed = require('blessed');
const contrib = require('blessed-contrib');
const fs = require('fs')
const transform = require('stream-transform');
const csv = require("fast-csv");
const uuidv4 = require('uuid/v4');
const mysql = require('mysql');
const vorpal = require('vorpal')();

// configs
var config = require('./config');

// globals
//show the gui
var gui = false;


const checkstring = '0123456789ACDEFGHJKLMNPRTUVWXY'

//number of cuncurrent http requests
const stepsize = 100;

//number of records proccessed
var count = 0;

//number of failed requests
var failcount = 0;

//number of successful requests
var successcount = 0;

//array for working data
var output = {}



if (gui) {
    //ui 
    var screen = blessed.screen()
    var grid = new contrib.grid({
        rows: 12,
        cols: 12,
        screen: screen
    })




    //var tree = grid.set(4,0,4,12, contrib.tree({fg: 'green'}));
    //screen.append(tree);
    //allow control the table with the keyboard


    var gauge = grid.set(0, 0, 4, 12, contrib.gauge, {
        label: 'Progress',
        stroke: 'green',
        fill: 'white'
    });
    screen.append(gauge);
    gauge.setPercent(0)
    gauge.setStack([{
        percent: 0,
        stroke: 'green'
    }, {
        percent: 0,
        stroke: 'magenta'
    }, {
        percent: 100,
        stroke: 'cyan'
    }])
    var log = grid.set(4, 1, 4, 10, contrib.log, {
        fg: "green",
        selectedFg: "green",
        label: 'Status'
    });
    screen.append(log);
    screen.key(['escape', 'q', 'C-c'], function(ch, key) {
        return process.exit(0);
    });
    screen.on('resize', function() {
        gauge.emit('attach');
        log.emit('attach');
    });
    var setGuage = function(percent_success, percent_fail, percent_left) {
        gauge.setStack(
            [{
                    percent: percent_success,
                    stroke: 'green'
                },
                {
                    percent: percent_fail,
                    stroke: 'magenta'
                },
                {
                    percent: percent_left,
                    stroke: 'cyan'
                }
            ]);
    }


    var tree = grid.set(6, 0, 4, 12, contrib.tree, {
        fg: 'green'
    })


    //allow control the table with the keyboard
    tree.focus()

    tree.on('select', function(node) {
        if (node.loadType) {
            handleNode(node.parent.name, node.loadType)
        }
        log.log(node.name);
    });

    // you can specify a name property at root level to display root
    tree.setData({
        extended: true,
        children: {
            'Concept': {
                children: {
                    'load': {
                        loadType: 'raw'
                    }
                }
            },
            'Patient': {
                children: {
                    'load': {
                        loadType: 'fhir'
                    }
                }
            }
        },
    })

    screen.key('q', function() {
        process.exit(0);
    });
    var getKids = function() {
        return {
            'foo': 'bar'
        }
    }


    screen.render()
} else {
    var setGuage = function(percent_success, percent_fail, percent_left) {}
    var log = console;
}
//convert to mysql dates
Date.prototype.toMysqlFormat = function() {
    function twoDigits(d) {
        if (0 <= d && d < 10) return "0" + d.toString();
        if (-10 < d && d < 0) return "-0" + (-1 * d).toString();
        return d.toString();
    }
    return this.getUTCFullYear() + "-" + twoDigits(1 + this.getUTCMonth()) + "-" + twoDigits(this.getUTCDate()) + " " + twoDigits(this.getHours()) + ":" + twoDigits(this.getUTCMinutes()) + ":" + twoDigits(this.getUTCSeconds());
};

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

var practitioner = {
    "resourceType": "Practitioner",
    "id": "a846f32f-5401-4d53-871a-68354c22c3f9",
    "name": {
        "family": [
            "Careful"
        ],
        "given": [
            "Adam"
        ]
    },
    address: [{
        use: "home",
        city: "E. Kanateng"
    }],
    gender: "MALE",
    birthDate: "2009-08-11T00:00:00"
}

// create fhir compatible patient object
var patientFormatter = function(record) {
    var uuid = record.PatientID.toLowerCase();
    var id = record.PatientID.substring(0, 10).replace(/B/g, 'H').replace('-', '');
    var OpenMRSID = id + luhn.generateCheckCharacter(id, checkstring);
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
            "system": "OpenMRS ID",
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
                "reference": "Practitioner/0bf6fc77-97ec-408b-b037-c039a7b9b7bc",
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
    var o = {
        "resourceType": "Observation",
        "code": {
            "coding": [{
                "system": "http://openmrs.org",
                "code": "3073f2ec-e632-4e47-a9e2-797fdae81452",
            }]
        },
        "subject": {
            "id": "2da6e30b-ff28-4788-87b9-8b7b59e986c1",
        },
        "effectiveDateTime": "2018-04-23T14:04:10-04:00",
        "valueQuantity": {
            "value": 15.4,
        }
    }
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

//openmrs specific format for lab values from concept record
conceptFormatter = function(record) {
    var concept_id = "1";
    var concept_uuid = "1";
    var description = record.name + " in " + record.system + " assay";
    var description_uuid = "1";
    var long_name = record.LabName;
    var short_name = record.name
    var hi_normal = record.max;
    var low_normal = record.min;
    var hi_absolute = record.max;
    var low_absolute = record.min;
    var hi_critical = record.max;
    var low_critical = record.min;
    var units = record.units;
    var date = new Date().toMysqlFormat(); //return MySQL Datetime format
    var daughters = [];
    var concept = {
        "datatype_id": "1",
        "date_created": date,
        "class_id": "1",
        "creator": "1",
        "uuid": uuidv4()
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
            "precise": "1",
            "units": units,
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

//remove fields we don't need to generate unique observation concepts
var observationTransformer = function(record) {
    delete record['PatientID'];
    delete record['AdmissionID'];
    delete record['LabDateTime'];
    return record;
}


//process text files for import
var importRawResource = function(resource) {
    var filename = config.files[resource]
    var stream = fs.createReadStream(filename);
    output[resource] = []
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
        var promises = output[resource].map(
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
                                log.log(error.sqlMessage);
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
            output[resource].push(data);
        })
        .on("end", function() {
            log.log("Running raw requests")
            rawRequest(resource)
        });
}

var postFhirResource = function(resource) {
    return handleFhirResource(resource, 'POST');
}
var putFhirResource = function(resource) {
    return handleFhirResource(resource, 'PUT');
}


//process text files for FHIR import
var handleFhirResource = function(resource, method) {
    function request(resource, count) {
        //console.log('request');
        //console.log({count});
        var handleRecord = function(record) {
            if (method == 'PUT') {
                var uri = config.url + '/fhir/' + resource + '/' + record.id;
            } else {
                var uri = config.url + '/fhir/' + resource;
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
            //  var deferred = Q.defer();
            //  return deferred.promise;
        }

        var promises = new Array(stepsize);
        if (count > output[resource].length) {
            return
        } else if (count == undefined) {
            count = 0
        }
        for (var i = 0; i < stepsize; i++) {
            var record = output[resource][count + i];
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
                log.log({
                    count,
                    stepsize
                });
                log.log({
                    successcount,
                    failcount
                });
                request(resource, count + stepsize);
                var percent_success = successcount / output[resource].length;
            })
            .catch(function(err) {
                console.log(err);
                request(resource, count + stepsize);
            });
    }

    var filename = config.files[resource]
    var stream = fs.createReadStream(filename);
    output[resource] = []
    csv
        .fromStream(stream, {
            headers: true,
            delimiter: '\t'
        })
        .transform(function(obj) {
            var transformed = transformRecord(obj, resource);
            return transformed;
        })
        .validate(function(data) {
            return data.resourceType !== undefined; //resources need type
        })
        .on("data-invalid", function(data) {
            log.log("invalid data")
        })
        .on("data", function(data) {
            output[resource].push(data);
        })
        .on("end", function() {
            log.log("done");
            log.log("Running sequential requests!")
            request(resource);
        });
}

var genConcepts = function(source, destination) {
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
        }
    }
    log.log("reading" + readfile)
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
            log.log("invalid data")
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
                log.log("processed " + total);
            }
        })
        .on("end", function() {
            log.log("done reading " + readfile);
            log.log("start writing " + writefile)
            csvStream.pipe(writestream);
            for (LabName in concepts) {
                var concept = formatter(concepts[LabName])
                concepts[LabName] = concept;
                csvStream.write(concept);
            }
            csvStream.end();
            writestream.on("finish", function() {
                log.log("done writing " + writefile);
                deferred.resolve();
            });
        });
    return deferred.promise;
}


console.log(process.argv.length);
if (process.argv.length >= 3) {
    if (process.argv[2] == 'genconcepts') {
        genConcepts('Observation', 'Concept');
        //} else 
    }

} else {

    //ui
    vorpal
        .command('genconcepts', 'Generate Lab Dictionary')
        .action(function(args, callback) {
            genConcepts('Observation', 'Concept');
            this.log('bar');
            callback();
        });

    vorpal
        .command('loadconcepts', 'Import Dictionary')
        .action(function(args, callback) {
            importRawResource('Concept');
            this.log('bar');
            callback();
        });

    vorpal
        .command('loadpatients', 'Import Patients')
        .action(function(args, callback) {
            postFhirResource('Patient');
            this.log('bar');
            callback();
        });

    vorpal
        .command('loadencounters', 'Import Visits')
        .action(function(args, callback) {
            postFhirResource('Encounter');
            this.log('bar');
            callback();
        });


    vorpal
        .command('loadlabs', 'Import Observations')
        .action(function(args, callback) {
            postFhirResource('Observation');
            this.log('bar');
            callback();
        });

    vorpal
        .delimiter('mrsman$')
        .show();
}