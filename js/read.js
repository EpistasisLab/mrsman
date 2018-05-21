#!/usr/bin/env node
 // basic requires
const http = require('http');
const Q = require("q"); // v2.11.1
const rp = require('request-promise');
const fs = require('fs')
const vorpal = require('vorpal')();
const util = require('util')
const path = require('path')
const Objects = require('./model');
//const cypher = require('cypher-stream')('bolt://localhost', 'neo4j', 'password');
var neo4j = require('neo4j-driver');
var driver = neo4j.v1.driver("bolt://127.0.0.1", neo4j.v1.auth.basic("neo4j", "password"));

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

var patient_cypher = fs.readFileSync(config.dirs.cypher + 'patient.cypher', 'utf8');




var p = readResource('DemoPatients');
p.then(function(data) {
    processPatients(data, 0).then(function(patients) {
        for (var i in patients) {
            var patient = patients[i];
            patient.sync(patient).then(function(data) {
                var json = JSON.stringify(data);
                var filename = path.resolve(config.dirs.Patient) + '/' + data.id + '.json';
                var nd = Q.defer();
                fs.writeFile(filename, json, 'utf8', function(result) {
                    var fn = "file://" + filename;
                    session = driver.session();
                    session
                        .run(patient_cypher, {
                            url: fn
                        })
                        .then(function(result) {
                            result.records.forEach(function(record) {
                                console.log(record.get('count(*)'));
                            });
                            nd.resolve('done');
                            //              session.close();
                        })
                        .catch(function(error) {
                            console.log(error);
                        });
                });
                nd.promise.then(function(foo) {
                })
            });
        }
    });
});
