var BaseModel = require('./base_model'),
    util = require('util'),
    Observation = require('./Observation');

function EncounterModel() {
    BaseModel.apply(this, arguments);
    this.resourceType = 'Encounter';
    if (this.Encounter !== undefined) {
        this.id = this.Encounter[0].id;
        delete this.Encounter
    }
    //format variables 
}
EncounterModel.prototype.getObs = function() {
    var deferred = Q.defer();
    for (var i in this.Observation){
          console.log(this.Observation[i]);
        var observation = new Observation(this.Observation[i])
        this.Observation[i] = observation.get();
    }
return this;
} 
EncounterModel.prototype.resolve_promises = function() {
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
            var observation = new Observation(data[i].value);
            that.Observation.push(observation);
        }
        deferred.resolve(that);
    })
    return deferred.promise;
};



util.inherits(EncounterModel, BaseModel);
module.exports = EncounterModel;
