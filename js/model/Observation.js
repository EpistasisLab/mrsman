var BaseModel = require('./Base'),
    util = require('util');
function ObservationModel() {
    BaseModel.apply(this, arguments);
    this.resourceType = 'Observation';
}
util.inherits(ObservationModel, BaseModel);
module.exports = ObservationModel;
