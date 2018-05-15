var BaseModel = require('./base_model'),
    util = require('util'),
    Encounter = require('./Encounter'),
    Q = require('q');

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
        delete this.identifier;
    }
    if (this.name) {
        for (var i in this.name) {
            this[i] = this.name[i];
        }
        this.firstname = this.name[0].given[0];
        this.lastname = this.name[0].family;
        delete this.name;
    }
    if (this.address) {
        this.city = this.address[0].city
        delete this.city;
    }
    if (this.extended) {
        for (var prop in this.extended) {
            if (prop === 'Encounter') {
                var promises = [];
                for (var i in this.extended[prop]) {
                    promises.push(new Encounter(this.extended[prop][i]).getextended());

                }

                //    console.log(promises);
                this[prop] = promises;
            }
        }

        delete this.extended;
    }

}

var handle_extended = function(data) {
    this.Encounter = [];
    for (var i in data) {
        var encounter = new Encounter(data[i].value);
        encounter.getObs();
        this.Encounter.push(encounter);
    }
    return this;
};

PatientModel.prototype.getObs = function() {
    for (i in this.Encounter) {
    this.Encounter[i].getObs();
    }
}

PatientModel.prototype.resolve_promises = function() {
    var deferred = Q.defer();
    var promises = []
    var that = this;
    //iterate over child-objects 
    for (key in this) {
        if (typeof(this[key]) === 'object') {
            var obj = this[key]
            for (var i in obj) {
                if (typeof(obj[i]) == 'object' && Q.resolve(obj[i]) == obj[i]) {
                    promises.push(obj[i]);
                }
            }
        }
    }
    Q.allSettled(promises).then(function(data) {
        that.Encounter = [];
        for (var i in data) {
            var encounter = new Encounter(data[i].value);
            that.Encounter.push(encounter);
        }
        deferred.resolve(that);
    })
    return deferred.promise;
};


util.inherits(PatientModel, BaseModel);
module.exports = PatientModel;
