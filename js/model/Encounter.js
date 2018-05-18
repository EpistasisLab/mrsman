var util = require('util');
var Q = require('q');
var Model = {};
Model['Base'] = require('./Base');
Model['Location'] = require('./Location');
Model['Practitioner'] = require('./Practitioner');
Model['Observation'] = require('./Observation');
var ObservationModel = require('./Observation');

function EncounterModel(obj) {
    Model['Base'].apply(this, arguments);
    this.resourceType = 'Encounter';
    this.Observation = Model['Observation'];
    this.Location = Model['Location'];
    this.Practitioner = Model['Practitioner'];
    this.format(obj)
}



EncounterModel.prototype.format = function(obj) {
    var obj = JSON.parse(JSON.stringify(obj));
    for (key in obj) {
        if (this[key] !== undefined) {
            if (typeof(this[key]) !== 'function') {
                this[key] = obj[key];
            } else {
                 var objects = [];
                 obj[key].map(function(data) {
                     objects.push(new Model[key](data));
                 });
                 this[key] = objects;
            }

            
        }
    }
    if (obj.id) { 
        this.id = obj.id;
    }
    if (this.resourceType !== undefined && obj[this.resourceType] !== undefined) {
        this.id = obj[this.resourceType][0]['id']
        delete obj[this.resourceType]
    }
}

util.inherits(EncounterModel, Model['Base']);
module.exports = EncounterModel;
