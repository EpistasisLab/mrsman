#!/usr/bin/env node
 // basic requires
const http = require('http');
const Q = require("q"); // v2.11.1
const rp = require('request-promise');
const fs = require('fs')
const vorpal = require('vorpal')();
const util = require('util')
const cypher = require('cypher-stream')('bolt://localhost', 'neo4j', 'password');
//var Patient = require('./model/Patient');
var Objects = {}
Objects['Patient'] = require('./model/Patient');
Objects['Encounter'] = require('./model/Encounter');
//var Observation = require('./model/Observation');
//var Visit = require('./model/Visit');

// configs
var config = require('./config');

// working memory 
var output = {};
var objects = [];
output['raw'] = {};

function readResource(resource) {
    var deferred = Q.defer();
    var filename = config.files[resource];
    var return_list = []
    if (fs.existsSync(filename)) {
        fs.readFile(filename, 'utf8', function(err, data) {
            if (err) throw err;
            var patients = [];
            var obj = JSON.parse(data);
            for (var i in obj.entry) {
                var record = obj.entry[i].resource;
                return_list.push(record);
            }
            deferred.resolve(return_list);
        });

    } else {
        deferred.reject(new Error("File Does not exist"));
    }
    return deferred.promise;


}

var processPatients = function(patients, i) {
    var deferred = Q.defer();
    if (i >= patients.length) {
        deferred.resolve(patients)
    } else {
        var patient = new Objects['Patient'](patients[i])
        patient.getextended().then(function(data) {
            patient.extended = data;
            console.log(i);
            patients[i] = patient;
            deferred.resolve(processPatients(patients, i + 1));
        })
    }
    return deferred.promise;

}




var p = readResource('DemoPatients');
p.then(function(data) {
    processPatients(data, 0).then(function(patients) {
        console.log('done patients');
        console.log(patients);
    });
});
