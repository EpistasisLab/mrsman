var BaseModel = require('./Base'),
    util = require('util');
function ConceptModel() {
    BaseModel.apply(this, arguments);
}
util.inherits(ConceptModel, BaseModel);

ConceptModel.prototype.formatter = function (record) { 
    var concept_id = "1";
    var concept_uuid = "1";
    var description = record.name + " in " + record.system + " assay";
    var description_uuid = "1";
    var long_name = record.LabName;
    var short_name = record.name
    var hi_normal = record.max;
    var low_normal = record.min;
    var hi_absolute = record.max;
    var low_absolute = record.min;
    var hi_critical = record.max;
    var low_critical = record.min;
    var units = record.units;
    var date = new Date().toMysqlFormat(); //return MySQL Datetime format
    var daughters = [];
    var concept = {
        "datatype_id": "1",
        "date_created": date,
        "class_id": "1",
        "creator": "1",
        "uuid": uuidv4()
    };
    daughters.push({
        "concept_description": {
            "concept_id": null,
            "date_created": date,
            "date_changed": date,
            "locale": "en",
            "creator": "1",
            "changed_by": "1",
            "description": description,
            "uuid": uuidv4()
        }
    });
    daughters.push({
        "concept_name": {
            "concept_id": null,
            "name": long_name,
            "date_created": date,
            "creator": "1",
            "locale": "en",
            "locale_preferred": "1",
            "concept_name_type": "FULLY_SPECIFIED",
            "uuid": uuidv4()
        }
    });
    daughters.push({
        "concept_name": {
            "concept_id": null,
            "name": short_name,
            "date_created": date,
            "creator": "1",
            "locale": "en",
            "locale_preferred": "0",
            "concept_name_type": "SHORT",
            "uuid": uuidv4()
        }
    });
    daughters.push({
        "concept_numeric": {
            "concept_id": null,
            "precise": "1",
            "units": units,
            "hi_normal": hi_normal,
            "low_normal": low_normal,
            "hi_absolute": hi_absolute,
            "low_absolute": low_absolute,
            "hi_critical": hi_critical,
            "low_critical": low_critical
        }
    });
    return {
        concept,
        daughters
    };
};
module.exports = ConceptModel;
