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
var getConcepts = function() {
    var deferred = Q.defer();
    session = neo.session();
    session
        .run('MATCH (p:Patient) WHERE p.uuid = "4ffc965e-3fa6-4bc8-8f67-139862b21b96" with p MATCH (o)-[:of]-(p) return distinct(o.concept_uuid)', {})
        .then(function(result) {
            var concepts = [];
            result.records.forEach(function(record) {
                //console.log(record.get('(o.concept)'));
                concepts.push(record.get('(o.concept_uuid)'))
            });
            deferred.resolve(concepts);
            session.close();
        })
        .catch(function(error) {
            console.log(error);
        });
    return deferred.promise;
}

var getConceptz = function() {
    var deferred = Q.defer();
    var return_list = []
    var method = 'GET';
    var uri = config.url + '/rest/v1/concept';
    var options = {
        method: method,
        uri: uri,
        resolveWithFullResponse: true,
        headers: {
            "Authorization": config.auth
        }
    };


    rp(options)
        .then(function(data) {
            var obj = JSON.parse(data.body);
            for (var i in obj.results) {
                var record = obj.results[i];
                return_list.push(record);
            }
            deferred.resolve(return_list);

        });
    return deferred.promise;
}



//function to generate patient object from raw data
var processConcepts = function(concepts, i) {
    //console.log(concepts);
    var deferred = Q.defer();
    if (i >= concepts.length) {
        deferred.resolve('fin')
        neo.close();
    } else {
        var concept = new Objects['Concept']({id:concepts[i]})
            var nd = Q.defer();
            var cr = concept.get_rest();
            cr.then(function(d) {
            //console.log(d);
//            nd.resolve(processConcepts(concepts, i + 1));
        if (!debug) {
//var json = d;
//console.log(d);
            var json = JSON.stringify(d);
            var filename = path.resolve(config.dirs.Concept) + '/' + concept.id + '.json';
            fs.writeFile(filename, json, 'utf8', function(result) {
                var fn = "file://" + filename;
                session = neo.session();
                session
                    .run(concept_cypher, {
                        url: fn
                    })
                    .then(function(result) {
                        result.records.forEach(function(record) {
                            console.log(record.get('count(*)'));
                        });
                        session.close();
                        nd.resolve(processConcepts(concepts, i + 1));
                        if (i == concepts.length) {
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
      //      console.log(JSON.stringify(concept));
                        nd.resolve(processConcepts(concepts, i + 1));
        }



            });



    }
    return deferred.promise;
}

//APOC data loader for patient json files
var concept_cypher = fs.readFileSync(config.dirs.cypher + 'concept.cypher', 'utf8');



//load patient data
//resource may be a file
var c = getConcepts();
c.then(function(data) {
    //console.log(data);
    //neo.close();
    var i = 0;
        processConcepts(data, 0).then(function(pt) {
    //        console.log(pt);
            neo.close();
        });

});
