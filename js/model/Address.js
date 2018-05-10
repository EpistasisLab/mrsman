var BaseModel = require('./base_model'),
    util = require('util');
//an address, city or any other kind of geographical domain
function PlaceModel() {
    this.resourceType = 'Place';
    BaseModel.apply(this, arguments);
}

}
util.inherits(PatientModel, BaseModel);
module.exports = PatientModel;
