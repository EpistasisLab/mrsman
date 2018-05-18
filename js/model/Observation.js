var BaseModel = require('./Base'),
    util = require('util');
function ObservationModel(obj) {
    BaseModel.apply(this, arguments);
    this.resourceType = 'Observation';
    this.format(obj);
}
util.inherits(ObservationModel, BaseModel);
module.exports = ObservationModel;
