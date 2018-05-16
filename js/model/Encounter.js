var util = require('util');
var Q = require('q');
var Model = {};
Model['Base'] = require('./Base');
Model['Location'] = require('./Location');
Model['Practitioner'] = require('./Practitioner');
Model['Observation'] = require('./Observation');

function EncounterModel() {
    Model['Base'].apply(this, arguments);
    this.resourceType = 'Encounter';
    if (this.Encounter !== undefined) {
        this.id = this.Encounter[0].id;
        delete this.Encounter
    }
//    if (this.Observation !== undefined) {
//        var observations = [];
//        for (var i in this.Observation) {
//            observations.push(new Model['Observation'](this.Observation[i]));
//        }
//        this.Observation = observations;
//    }

}
util.inherits(EncounterModel, Model['Base']);
module.exports = EncounterModel;
