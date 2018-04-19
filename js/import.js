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
//number of cuncurrent http requests
const poolsize = 10;
//
//number of records proccessed
var count = 0;
//number of failed requests
var failcount = 0;

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
screen.render()

var fhirPatient = function(record) {
    var gender = record.PatientGender.toLowerCase();
    var identifier = record.PatientID.toLowerCase()
    var birthdate = new Date(record.PatientDateOfBirth).toISOString().substr(0, 10);
    var id = record.PatientID.substring(0, 8).replace(/B/g, 'H');
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


var importPatient = function(patient) {
    var options = {
        method: 'POST',
        uri: config.url,
        headers: {
            "Authorization": config.auth
        },
        json: patient // Automatically stringifies the body to JSON
    };
    var promise = rp(options)
    return (promise);
}

var to = 100;
var from = 0;
var output = [];
//var parser = parse({delimiter: '\t',to: to,from: from})
//var filename = '../data/PatientCorePopulatedTable_short.txt';
var filename = '../data/PatientCorePopulatedTable.txt';
//input.pipe(parser).pipe(transformer).pipe(process.stdout);
//var input = '#Welcome\n"1","2","3","4"\n"a","b","c","d"';
//console.log(output);
//parse(input, {delimiter: '\t',to: to,from: from}, function(err, output){
//});

/*
var formatStream = csv
        .createWriteStream({headers: true})
        .transform(function(obj){
            var patient=formatPatient(obj);
            return patient;
        });
csv
   .fromPath(filename, {headers: true, delimiter: '\t'})
   .pipe(formatStream)
*/
function httpRequest(count) {
    if (count == undefined) {
        count = 0
    }
    count++
    var percent_success = ((count - failcount) / output.length).toFixed(2);
    var percent_fail = (failcount / output.length).toFixed(2);
    var percent_left = ((output.length - count) / output.length).toFixed(2);
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
    if (count > output.length) {
        return
    }





    var patient = output[count];
    var p = importPatient(patient);
    p.then(function(parsedBody) {
            log.log('success')
            sequentialRequest(count);
            // POST succeeded...
        })
        .catch(function(err) {
            failcount++
            log.log('fail')
            httpRequest(count);
        });



}


var stream = fs.createReadStream(filename);
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
        //do something with invalid row
    })
    .on("data", function(data) {
        //importPatient(data);
        output.push(data);
    })
    .on("end", function() {
        console.log("done");
        total = output.length;
        console.log("Running sequential requests!")
        httpRequest()



    });
