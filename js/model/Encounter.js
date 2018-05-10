var BaseModel = require('./base_model'),
    util = require('util');

function EncounterModel() {
    this.resourceType = 'Encounter';
    BaseModel.apply(this, arguments);
}

util.inherits(EncounterModel, BaseModel);
module.exports = EncounterModel;
