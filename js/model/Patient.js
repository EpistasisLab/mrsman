var BaseModel = require('./Base'),
    util = require('util'),
    Encounter = require('./Encounter'),
    Q = require('q');

function PatientModel(obj) {
    BaseModel.apply(this, arguments);
    this.resourceType = 'Patient';
    this.firstname = '';
    this.lastname = '';
    this.birthDate = '',
        this.deceasedBoolean = '',
        this.gender = '',
        this.city = '';
    this.format(obj);
}



//fetch all objects associated with a patient
PatientModel.prototype.sync = function() {
    var deferred = Q.defer();
    var promises = []
    var patient = this;
    //iterate over child-objects 
    for (key in patient) {
        if (typeof(patient[key]) === 'object') {
            var obj = patient[key]
            for (var i in obj) {
                if (typeof(obj[i]) == 'object' && Q.resolve(obj[i]) == obj[i]) {
                    promises.push(obj[i]);
                }
            }
        }
    }
    Q.allSettled(promises).then(function(data) {
        patient.Encounter = [];
        for (var i in data) {
            if (Object.getOwnPropertyNames(data[i].value).length > 0) {
                var encounter = new Encounter(data[i].value);
                patient.Encounter.push(encounter);
            }
        }
        deferred.resolve(patient);
    })
    return deferred.promise;
};


PatientModel.prototype.format = function(obj) {
    var obj = JSON.parse(JSON.stringify(obj));
    for (var i in obj) {
        if (this[i] == '') {
            this[i] = obj[i];
        }
    }
    if (obj.id) {
        this.id = obj.id;
    }
    if (obj.identifier) {
        for (var i in obj.identifier) {
            if (obj.identifier[i].system == 'OpenMRS Identification Number') {
                this.openmrs_id = obj.identifier[i].value;
            }
        }
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


util.inherits(PatientModel, BaseModel);
module.exports = PatientModel;