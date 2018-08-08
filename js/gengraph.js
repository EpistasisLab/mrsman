#!/usr/bin/env node
// load data from OpenMRS into neo4j based on a userlist
// basic requires
const http = require('http');
const Q = require("q"); // v2.11.1
const rp = require('request-promise');
const fs = require('fs')
const vorpal = require('vorpal')();
const util = require('util')
const path = require('path')
const Objects = require('./model');
const debug = false; 
// configs
var config = require('./config');

// db
var neo4j = require('neo4j-driver');
var neo = neo4j.v1.driver(config.neo_uri, neo4j.v1.auth.basic(config.neo_user, config.neo_pass));


//load data from a resource defined in config
function readResource(resource) {
    var deferred = Q.defer();
    var return_list = []
    if (config.json_files[resource] !== undefined && fs.existsSync(config.json_files[resource])) {
        fs.readFile(config.json_files[resource], 'utf8', function(err, data) {
            if (err) throw err;
            var obj = JSON.parse(data);
            if(obj.entry) {
                for (var i in obj.entry) {
                    var record = obj.entry[i].resource;
                    return_list.push(record);
                }
            } else {
                return_list  = obj;
            }
            deferred.resolve(return_list);
        });

    } else {
        getPatients().then(function(data) {
            var obj = JSON.parse(data.body);
            for (var i in obj.entry) {
                var record = obj.entry[i].resource;
                return_list.push(record);
            }
            deferred.resolve(return_list);
        });
    }
    return deferred.promise;
}




var getPatients = function() {
    var method = 'GET';
    var uri = config.url + '/fhir/Patient?active=true';
    var options = {
        method: method,
        uri: uri,
        resolveWithFullResponse: true,
        headers: {
            "Authorization": config.auth
        }
    };
    return rp(options)
}



//function to generate patient object from raw data
var processPatients = function(patients, i) {
    var deferred = Q.defer();
    if (i >= patients.length) {
        deferred.resolve('fin')
        neo.close();
    } else {
        var patient = new Objects['Patient'](patients[i])
        patient.get_extended().then(function(data) {
            patient.extended = data;
            patient = new Objects['Patient'](patient)
            //deferred.resolve(patient);

            patient.sync(patient).then(function(data) {
                if (!debug) {
                    var json = JSON.stringify(data);
                    var filename = path.resolve(config.dirs.Patient) + '/' + data.id + '.json';
                    var nd = Q.defer();
                    fs.writeFile(filename, json, 'utf8', function(result) {
                        var fn = "file://" + filename;
                        session = neo.session();
                        session
                            .run(patient_cypher, {
                                url: fn
                            })
                            .then(function(result) {
                                result.records.forEach(function(record) {
                                    console.log(record.get('count(*)'));
                                });
                                session.close();
                                nd.resolve(processPatients(patients, i + 1));
                                if (i == patients.length) {
                                    neo.close();
                                }
                            })
                            .catch(function(error) {
                                console.log(error);
                            });
                    });
                    nd.promise.then(function(foo) {
                        console.log(foo);
                    })
                } else {
                    console.log(JSON.stringify(data));
                }

            });





        })
    }
    return deferred.promise;
}

//APOC data loader for patient json files
var patient_cypher = fs.readFileSync(config.dirs.cypher + 'patient.cypher', 'utf8');



//load patient data
//resource may be a file
//var p = readResource('DemoPatients');
var p = readResource('Patient');
p.then(function(data) {
    var i = 0;
    processPatients(data, 0).then(function(pt) {
        console.log(pt);
        neo.close();
        neo.close();
    });




});
