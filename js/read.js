#!/usr/bin/env node
 // basic requires
const http = require('http');
const Q = require("q"); // v2.11.1
const rp = require('request-promise');
const fs = require('fs')
const vorpal = require('vorpal')();
const util = require('util')
//const cypher = require('cypher-stream')('bolt://localhost', 'neo4j', 'password');
const neo4j = require('node-neo4j');
var db = new neo4j('http://neo4j:password@localhost:7474');
const Objects = require('./model');


// configs
var config = require('./config');

// working memory 
var output = [];

function readResource(resource) {
    var deferred = Q.defer();
    var filename = config.files[resource];
    var return_list = []
    if (fs.existsSync(filename)) {
        fs.readFile(filename, 'utf8', function(err, data) {
            if (err) throw err;
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
        patient.get_extended().then(function(data) {
            patient.extended = data;
            patient = new Objects['Patient'](patient)
            patients[i] = patient;
            deferred.resolve(processPatients(patients, i + 1));
        })
    }
    return deferred.promise;

}




var p = readResource('DemoPatients');
p.then(function(data) {
    processPatients(data, 0).then(function(patients) {
        promises = [];
        for (var i in patients) {
            var promise = Q.defer();
            promises.push(promise);
            var patient = patients[i];
            patient.sync(patient).then(function(data) {
                var json = JSON.stringify(data);
console.log(data.id);
                fs.writeFile(config.patient_dir +  '/' + data.id + '.json', json, 'utf8', function(data){console.log('done')});
            });
        }
    });
});
