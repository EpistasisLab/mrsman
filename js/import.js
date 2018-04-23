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



//convert patien to fhir syntax
var fhirPatient = function(record) {
    var gender = record.PatientGender.toLowerCase();
    var identifier = record.PatientID.toLowerCase()
    var birthdate = new Date(record.PatientDateOfBirth).toISOString().substr(0, 10);
    var id = record.PatientID.substring(0, 7).replace(/B/g, 'H');
    var OpenMRSID = id + luhn.generateCheckCharacter(id, '0123456789ACDEFGHJKLMNPRTUVWXY');
    var givenName = randomname({
        random: Math.random,
        first: true,
        gender: gender
    })
    var familyName = randomname({
        random: Math.random,
        last: true
    })

    return {
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

}


function httpRequest(count,resource) {
    var postRecord = function(record) {
        var options = {
            method: 'POST',
            uri: config.url + resource,
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
        promises[i] = postRecord(record);
    }
    Q.allSettled(promises)
        .then(function(results) {
            results.forEach(function(result) {
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
            httpRequest(count + stepsize,resource);
        });
}


var streamResource = function(resource) {
    var filename = config.files[resource]
    var stream = fs.createReadStream(filename);
    output[resource] = {}

csv
    .fromStream(stream, {
        headers: true,
        delimiter: '\t'
    })
    .transform(function(obj) {
        var patient = fhirPatient(obj);
        return patient;
    })
    .validate(function(data) {
        return data.resourceType !== undefined; //all persons must be under the age of 50
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
        httpRequest()
    });
}
