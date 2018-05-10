var BaseModel = require('./base_model'),
    util = require('util');

function PatientModel() {
    this.resourceType = 'Patient';
    BaseModel.apply(this, arguments);
    //format variables 
    if (this.identifier) {
        for (var i in this.identifier) {
            if (this.identifier[i].system == 'OpenMRS Identification Number') {
                this.openmrs_id = this.identifier[i].value;
            }
        }
    }
    if (this.name) {
        for (var i in this.name) {
            this[i] = this.name[i];
        }
        this.firstname = this.name[0].given[0];
        this.lastname = this.name[0].family;
    }
    if (this.address) {
        this.city = this.address[0].city
    }
    //this.city = this.address.city;

}

util.inherits(PatientModel, BaseModel);
module.exports = PatientModel;
