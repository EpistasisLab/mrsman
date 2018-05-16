var BaseModel = require('./Base'),
    util = require('util');
//an address, city or any other kind of geographical domain
function AddressModel() {
    this.resourceType = 'Address';
    BaseModel.apply(this, arguments);
}

}
util.inherits(AddressModel, BaseModel);
module.exports = AddressModel;
