var BaseModel = require('./Base'),
    util = require('util');

function PractitionerModel() {
    this.resourceType = 'Practitioner';
    BaseModel.apply(this, arguments);
}

util.inherits(PractitionerModel, BaseModel);
module.exports = PractitionerModel;
