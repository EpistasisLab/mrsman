var BaseModel = require('./Base'),
    util = require('util');
function LocationModel() {
    BaseModel.apply(this, arguments);
    this.resourceType = 'Location';
}
util.inherits(LocationModel, BaseModel);
module.exports = LocationModel;
