var BaseModel = require('./Base'),
    util = require('util');
function LocationModel(obj) {
    BaseModel.apply(this, arguments);
    this.resourceType = 'Location';
    this.format(obj);
}
util.inherits(LocationModel, BaseModel);
module.exports = LocationModel;
