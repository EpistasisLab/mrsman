var BaseModel = require('./Base'),
    util = require('util');


function formatRaw(obj) {
        if (obj.raw.valueQuantity !== undefined) {
            obj.value = formatQuantity(obj.raw.valueQuantity);
            var unit = formatUnit(obj.raw.valueQuantity);
            if (unit.length > 0) {
                obj.unit = unit;
            }
        } else if (obj.raw.valueString !== undefined) {
            obj.value = obj.raw.valueString[0]['$'].value
        } else if (obj.raw.valueCodeableConcept !== undefined) {
            var code = formatCode(obj.raw.valueCodeableConcept);
            obj.value = code.display;
        }
        if (obj.raw.code !== undefined) {
            var code = formatCode(obj.raw.code);
            obj.display = code.display;
            obj.concept = code.uuid;
        }
        if (obj.raw.effectiveDateTime !== undefined) {
            obj.date = obj.raw.effectiveDateTime[0]['$']['value'];
        }
        delete obj.raw;
        return obj

}

//convert raw xml tagged json to json
function formatQuantity(raw) {
    var value = '';
    if (raw[0] !== undefined && raw[0].value !== undefined && raw[0].value[0] !== undefined && raw[0].value[0]['$'] !== undefined && raw[0].value[0]['$'].value !== undefined) {
        value = raw[0].value[0]['$'].value;
    }
    return value;
}

function formatUnit(raw) {
    var unit = '';
    if (raw[0] !== undefined && raw[0].unit !== undefined && raw[0].unit[0] !== undefined && raw[0].unit[0]['$'] !== undefined && raw[0].unit[0]['$'].value !== undefined) {
        unit = raw[0].unit[0]['$'].value;
    }
    return unit;
}


function formatCode(code) {
    var display = '';
    var uuid = '';
    for (var i in code) {
        if (code[i].coding !== undefined) {
            for (var j in code[i].coding) {
                if (code[i].coding[j].display !== undefined) {
                    display = code[i].coding[j].display[0]['$'].value
                }
                if (code[i].coding[j].code !== undefined) {
                    uuid = code[i].coding[j].code[0]['$'].value
                }
            }
        }
    }
    return {display,uuid};
}

function ObservationModel(obj) {
    BaseModel.apply(this, arguments);
    this.resourceType = 'Observation';
    this.value = '';
    this.concept = '';
    this.unit = '';
    this.date = '';
    this.display = '';
    if (obj.raw !== undefined) {
        obj = formatRaw(obj);
    }
    this.format(obj);
}
util.inherits(ObservationModel, BaseModel);
module.exports = ObservationModel;
