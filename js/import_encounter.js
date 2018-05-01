// import patients from example dataset provided by:
// "A 100,000-patient database that contains in total 100,000 patients, 361,760 admissions, and 107,535,387 lab observations."
// http://arxiv.org/pdf/1608.00570.pdf 

// basic requires
var http = require('http');
var mysql = require('mysql');
var Q = require("q"); // v2.11.1
// record creation
var luhn = require('luhn-mod-n');
var randomname = require('node-random-name')
var rp = require('request-promise');
//ui
var blessed = require('blessed');
var contrib = require('blessed-contrib');
// configs
var config = require('./config');

// Define our connection to the MySQL database.
var pool = mysql.createPool({
    host: config.mysql_host,
    user: config.mysql_user,
    password: config.mysql_pass,
    database: config.mysql_dbname,
    connectionLimit: 20, // Default value is 10.
    waitForConnections: true, // Default value.
    queueLimit: 0 // Unlimited - default value.
});

var db = {
    query: function(sql, params) {
        var deferred = Q.defer();
        pool.query(sql, params, deferred.makeNodeResolver());
        return (deferred.promise);
    }
};

//ui 

/*

var screen = blessed.screen()

var grid = new contrib.grid({
    rows: 12,
    cols: 12,
    screen: screen
})

//var gauge = grid.set(1, 1, 2, 4, contrib.gauge, {label: 'Progress', stroke: 'green', fill: 'white'})
var gauge = grid.set(0, 0, 4, 12, contrib.gauge, {
    label: 'Progress',
    stroke: 'green',
    fill: 'white'
});
screen.append(gauge);

gauge.setPercent(0)

var log = grid.set(4, 1, 4, 10, contrib.log, {
    fg: "green",
    selectedFg: "green",
    label: 'Status'
});

screen.append(log);



screen.key(['escape', 'q', 'C-c'], function(ch, key) {
    return process.exit(0);
});

screen.on('resize', function() {
    gauge.emit('attach');
    log.emit('attach');
});

screen.render()
*/

// global
//number of records to process/thread
var batchsize = 1;

//number of records to process
var total = 0;

//number of records proccessed
var count = 0;

//format source patient record and import
var formatEncounter = function(record) {
var encounter = {
  "resourceType": "Encounter",
  "id": "4d9fe99c-09d2-4a55-b3f6-c87b05ec1517",
  "status": "finished",
  "type": [
    {
      "coding": [
        {
          "display": "Vitals"
        }
      ]
    }
  ],
  "subject": {
    "id": "eb552673-a137-4e52-b488-cd662de3e9f5",
    "reference": "Patient/eb552673-a137-4e52-b488-cd662de3e9f5",
    "display": "William Sopha(Identifier:904027a9-e42c-4501-95ad-ad86eed2e888)"
  },
  "participant": [
    {
      "individual": {
        "reference": "Practitioner/8cd6edbe-c5fd-4850-8841-86bd89606b81",
        "display": "Super User(Identifier:UNKNOWN)"
      }
    }
  ],
  "period": {
    "start": "2018-04-11T13:35:16-04:00",
    "end": "2018-04-11T13:35:16-04:00"
  },
  "location": [
    {
      "location": {
        "reference": "Location/b1a8b05e-3542-4037-bbd3-998ee9c40574",
        "display": "Inpatient Ward"
      },
      "period": {
        "start": "2018-04-11T13:35:16-04:00",
        "end": "2018-04-11T13:35:16-04:00"
      }
    }
  ],
  "partOf": {
    "reference": "Encounter/9ef9d287-cea7-4cae-8758-1d802b38b981",
    "display": "Facility Visit"
  }

}
return encounter;
}



var importBatch = function() {
    var pp = Q.defer();
    var sql = 'SELECT Admissions.*,OpenMRSPatients.UUID from Admissions left join Patients on Admissions.PatientID = Patients.PatientID left join OpenMRSPatients on Patients.PatientID = OpenMRSPatients.PatientID  where OpenMRSPatients.UUID is not null';

    sql += ' limit ' + batchsize + ';';
    db.query(sql).then(
        function handleSettled(snapshots) {
            if (snapshots[0].length > 0) {
                var promises = snapshots[0].map(
                    function iterator(row) {
                        var encounter = formatEncounter(row);
                        var options = {
                            method: 'POST',
                            uri: config.fhirbase + '/Encounter',
                            headers: {
                                "Authorization": config.auth
                            },
                            json: encounter // Automatically stringifies the body to JSON
                        };

                        var promise = rp(options)
                            .then(function(parsedBody) {
                                var retArray = Object.assign(parsedBody, patient, row)
                                return (retArray);
                            })
                            .catch(function(err) {
                                return (false);
                                console.log(err);
                                // POST failed...
                            })
                        return (promise);
                    }
                ); // END: Map loop.
            } else {
                log.log('done');
                pool.end()
                return;
            }

        }
    )
                            .catch(function(err) {
console.log(err);
                pool.end()
                                pp.reject(err);
                                // POST failed...
    });
    return (pp);
}
importBatch();
