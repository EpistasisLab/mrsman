#!/usr/bin/env node
 // import patients from example dataset provided by:
// "A 100,000-patient database that contains in total 100,000 patients, 361,760 admissions, and 107,535,387 lab observations."
// http://arxiv.org/pdf/1608.00570.pdf 

// basic requires
const http = require('http');
const Q = require("q"); // v2.11.1
const randomname = require('node-random-name')
const rp = require('request-promise');
const fs = require('fs')
const pgpool = require('pg-pool')
const mysql = require('mysql');
const luhn = require('luhn-mod-n');
const uuidv4 = require('uuid/v4');

//const checkstring = '0123456789ACDEFGHJKLMNPRTUVWXY';
// configs
var config = require('./config');
// models
var Patient = require('./model/Patient');
var Concept = require('./model/Concept');

//number of cuncurrent http or database requests 
var stepsize = 20;

//number of records proccessed
var count = 0;

//number of failed requests
var failcount = 0;

//number of successful requests
var successcount = 0;

// Define the connection to MySQL database.
var pg_pool = new pgpool({
    host: config.pg_host,
    user: config.pg_user,
    password: config.pg_pass,
    database: config.pg_dbname,
    max: 20, // set pool max size to 20
    min: 4, // set min pool size to 4
    idleTimeoutMillis: 1000, // close idle clients after 1 second
    connectionTimeoutMillis: 1000 // return an error after 1 second if connection could not be established
});


//helper function for db queries
var pg_db = {
    query: function(sql, params) {
        var deferred = Q.defer();
        pg_pool.query(sql, params, deferred.makeNodeResolver());
        return (deferred.promise);
    }
};
var loadPatients = function() {
    sql = "select patients.row_id,uuids.uuid patient_uuid,patients.gender,patients.dob,patients.dod,patients.dod_hosp,patients.dod_ssn,expire_flag from patients left join uuids on uuids.row_id = patients.row_id where uuids.src = 'patients' order by random() limit 2"
    var dbpromise = pg_db
        .query(sql)
        .then(
            function handleResponse(results) {
                console.log(results.rows.length);
                handlePatients(results.rows, stepsize);
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

}


var handlePatients = function(records, stepsize) {
    var promise = Q.defer();
    for (var i = 0; i < stepsize; i++) {
        var record = records.pop();
        var patient = handlePatient(record);
        //        patient.post();
        console.log(patient);
    }
    console.log(records.length);
    if (records.length > 0) {
//         handlePatients(records,stepsize);
    }
}



var handlePatient = function(obj) {
    var patient = {};
    patient.resourceType = 'Patient';
    patient.gender = {
        'M': 'male',
        'F': 'female'
    }[obj.gender]
//    patient.deceasedBoolean = {
//        '1': true,
//        '0': false
//    }[obj.expire_flag]
    var givenName = randomname({
        random: Math.random,
        first: true,
        gender: patient.gender
    });
    var familyName = randomname({
        random: Math.random,
        last: true
    });
    patient.id = obj.patient_uuid;
    var num = obj.row_id;
    var checkdigit = luhnCheckDigit(num);
    //    var id = obj.uuid.substring(0, 7).replace(/B/g, 'H');
    //   var OpenMRSID = id + luhn.generateCheckCharacter(id, checkstring);
    var OpenMRSID = num + '-' + checkdigit;
    patient.birthDate = new Date(obj.dob).toISOString().substr(0, 10);
    //   patient.birthdate = '';
    patient.identifier = [{
        "use": "usual",
        "system": "OpenMRS Identification Number",
        "value": OpenMRSID
    }];
    patient.name = [{
        "use": "usual",
        "family": familyName,
        "given": [
            givenName
        ]
    }];
//  patient.birthDate="1975-01-01";
  patient.deceasedBoolean=false;
  patient.address = [
    {
      "use": "home",
      "line": [
        "555 Johnson Rd.",
        "Apt. 555"
      ],
      "city": "Indianapolis",
      "state": "IN",
      "postalCode": "46202",
      "country": "USA"
    }
   ]
    patient.active = true;


    return(postPatient(patient));
}


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




//post object to FHIR interface
var postPatient = function(patient) {
    var deferred = Q.defer();
    console.log('posting ' + patient.resourceType);
    var uri = config.url + '/fhir/' + patient.resourceType;
    var method = 'POST';
    var options = {
        method: method,
        uri: uri,
        resolveWithFullResponse: true,
        headers: {
            "Authorization": config.auth
        },
        json: patient
    };
    console.log(options);
    var promise = rp(options)
    promise.then(function(data) {
        deferred.resolve(data.body);
    });
    return deferred.promise;
}
loadPatients();
