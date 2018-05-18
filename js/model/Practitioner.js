var BaseModel = require('./Base'),
    util = require('util');

function PractitionerModel(obj) {
    BaseModel.apply(this, arguments);
    this.resourceType = 'Practitioner';
    this.format(obj);
}

util.inherits(PractitionerModel, BaseModel);
module.exports = PractitionerModel;
