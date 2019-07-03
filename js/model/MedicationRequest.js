var BaseModel = require('./Base'),
    util = require('util'),
    Encounter = require('./Encounter'),
    Q = require('q');

function MedicationRequestModel(obj) {
    BaseModel.apply(this, arguments);
    this.resourceType = 'MedicationRequest';
    if(obj !== undefined) {
      this.format(obj);
    }
}

//fetch all objects associated with a patient
MedicationRequestModel.prototype.format = function(obj) {
    var obj = JSON.parse(JSON.stringify(obj));
    for (var i in obj) {
        if (this[i] == '') {
            this[i] = obj[i];
        }
    }
    if (obj.id) {
        this.id = obj.id;
    }
    if (obj.name) {
        this.firstname = obj.name[0].given[0];
        this.lastname = obj.name[0].family;
    }
    if (obj.address) {
        this.city = obj.address[0].city
    }
    if (obj.extended) {
        for (var prop in obj.extended) {
            if (prop === 'Encounter') {
                var promises = [];
                for (var i in obj.extended[prop]) {
                    promises.push(new Encounter(obj.extended[prop][i]).get_extended());

                }
                this.Encounter = promises;
            }
        }
    }
}

util.inherits(MedicationRequestModel, BaseModel);
module.exports = MedicationRequestModel;
