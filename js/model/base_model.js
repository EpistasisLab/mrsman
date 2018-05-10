const cypher = require('cypher-stream')('bolt://localhost', 'neo4j', 'password');
const config = require('../config');
const rp = require('request-promise');
const Q = require("q"); // v2.11.1
const xml2js = require('xml2js');


function BaseModel(obj) {
    this.resourceType = 'Base'
    for (key in obj) {
        if (key != 'use') {
            this[key] = obj[key];
        }
    }
}
BaseModel.prototype.fromID = function() { /* ... */ };


BaseModel.prototype.gencypher = function() {
    var params = [];
    //console.log(this);
    for (key in this) {
        if (['string', 'boolean'].indexOf(typeof(this[key])) >= 0) {
            params.push(key + ': \"' + this[key] + '\"');
        }
    }
    var create = 'CREATE (n:' + this.resourceType + '  { ' + params.join() + '})';
    return create;

};



BaseModel.prototype.getextended = function() {
    var deferred = Q.defer();
    console.log(this.resourceType);
   if(['Patient','Encounter'].indexOf(this.resourceType) >= 0) {
    var parseextended = function(data) {
        var entries = data.Bundle.entry
        var parsed = {}
        for (var i in entries) {
            var resource = entries[i].resource
            var resource_type = Object.keys(resource[0])[0];
            var resource_value = Object.values(resource[0])[0];
            if (parsed[resource_type] === undefined) {
                parsed[resource_type] = [];
            }
            parsed[resource_type].push(resource_value[0]);
        }
        return parsed;
    };
    //   var updated = []
    var uri = config.url + '/fhir/' + this.resourceType + '/' + this.id + '/$everything';
    var method = 'POST';
    var body = '<Parameters xmlns="http://hl7.org/fhir"/>';
    var options = {
        method: method,
        uri: uri,
        resolveWithFullResponse: true,
        body: body,
        headers: {
            'Authorization': config.auth,
            'Content-Type': 'text/xml',
            'Content-Length': Buffer.byteLength(body)
        },
    };
    var promise = rp(options)
    var that = this;
    promise.then(function(result) {
        if (result.statusCode == 200) {

            xml2js.parseString(result.body, {
                trim: true
            }, function(err, extended) {
                deferred.resolve(parseextended(extended));
            });

        } else {
            deferred.resolve();
        }
    }).catch(function(err, rr) {
        console.log('err')
        deferred.resolve();
    });
}
    return deferred.promise;

};





module.exports = BaseModel;
