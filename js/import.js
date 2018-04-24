// import patients from example dataset provided by:
// "A 100,000-patient database that contains in total 100,000 patients, 361,760 admissions, and 107,535,387 lab observations."
// http://arxiv.org/pdf/1608.00570.pdf 

// basic requires
const http = require('http');
const Q = require("q"); // v2.11.1
// record creation
const luhn = require('luhn-mod-n');
const randomname = require('node-random-name')
const rp = require('request-promise');
const request = require('request');
//ui
const blessed = require('blessed');
const contrib = require('blessed-contrib');
const fs = require('fs')
const transform = require('stream-transform');
const csv = require("fast-csv");

// configs
var config = require('./config');

// globals
//show the gui
var gui = false;

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
    screen.render()
} else {
    var setGuage = function(percent_success, percent_fail, percent_left) {}
    var log = console;
}


//convert record to fhir syntax based on resource type
var resourceTrans = function(record, resource) {
    var transformer = {}
    transformer['Observation'] = observationTransformer;
    return transformer[resource](record);
}





//convert record to fhir syntax based on resource type
var fhirRecord = function(record, resource) {
    var checkstring = '0123456789ACDEFGHJKLMNPRTUVWXY'
    //generate a patient id using first 8 chars, replace 'B' with 'H'
    var id = record.PatientID.substring(0, 7).replace(/B/g, 'H');
    var OpenMRSID = id + luhn.generateCheckCharacter(id, checkstring);
    var formatter = {};
    formatter['Patient'] = patientFormatter;
    formatter['Encounter'] = encounterFormatter;
    formatter['Observation'] = observationFormatter;
    return formatter[resource](record);
}


var patientFormatter = function(record) {
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


var encounterFormatter = function(record) {
    var encounter = {
        "resourceType": "Encounter",
        "type": [{
            "coding": [{
                "display": "Vitals"
            }]
        }],
        "subject": {
            "id": "69f895f8-5929-4be4-8164-bd7fd581e19d",
        },
        "participant": [{
            "individual": {
                "reference": "Practitioner/328797d7-196e-4344-9c12-6615bf26cc97",
            }
        }],
        "period": {
            "start": "2018-04-23T10:58:10-04:00",
            "end": "2018-04-23T10:58:10-04:00"
        },
        "location": [{
            "location": {
                "reference": "Location/b1a8b05e-3542-4037-bbd3-998ee9c40574",
            },
            "period": {
                "start": "2018-04-23T10:58:10-04:00",
                "end": "2018-04-23T10:58:10-04:00"
            }
        }],
        "partOf": {
            "reference": "Encounter/81df0058-6c93-4238-859d-8e63fee7766c",
        }
    }
    var visit = {
        "resourceType": "Encounter",
        "subject": {
            "id": "f199a94a-a726-47b4-b660-15b45ded6045",
        },

        "type": [{
            "coding": [{
                "code": "1",
            }]
        }],

        "period": {
            "start": "2018-04-23T10:33:10-04:00"
        },
        "location": [{
            "location": {
                "reference": "Location/aff27d58-a15c-49a6-9beb-d30dcfc0c66e",
                "display": "Amani Hospital"
            },
            "period": {
                "start": "2018-04-23T10:33:10-04:00"
            }
        }]
    }
    return visit;
}



observationFormatter = function(record) {
    var observation = {
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
    return observation;
}



var observationTransformer = function(record) {
    //var catandname = record['LabName'].split(": ");
    //var lab_name = catandname[1];
    delete record['PatientID'];
    delete record['AdmissionID'];
    delete record['LabDateTime'];
    return record;
}





function httpRequest(resource, count) {
    var postRecord = function(record) {
        var options = {
            method: 'POST',
            uri: config.url + '/' + resource,
            headers: {
                "Authorization": config.auth
            },
            json: record // Automatically stringifies the body to JSON
        };
        var promise = rp(options)
        return (promise);
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
            promises[i] = postRecord(record);
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
            httpRequest(resource, count + stepsize);
        });
}

var importResource = function(resource) {
    var filename = config.files[resource]
    var stream = fs.createReadStream(filename);
    output[resource] = []
    csv
        .fromStream(stream, {
            headers: true,
            delimiter: '\t'
        })
        .transform(function(obj) {
            var transformed = fhirRecord(obj, resource);
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
            console.log("done");
            total = output[resource].length;
            console.log("Running sequential requests!")
            httpRequest(resource)
        });
}

var genConcepts = function(source, destination) {
    var readfile = config.files[source]
    var writefile = config.files[destination]
    var readstream = fs.createReadStream(readfile);
    var writestream = fs.createWriteStream(writefile);
    var concepts = {}
    csv
        .fromStream(readstream, {
            headers: true,
            delimiter: '\t'
        })
        .transform(function(obj) {
            var transformed = resourceTrans(obj, resource);
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
        })
        .on("end", function() {
            console.log("done");
            for (concept in concepts) {
                console.log(concept);
            }

        });
}


genConcepts('Observation', 'Concept');

//importResource('Patient');
//importResource('Observation');
//importResource('Encounter');