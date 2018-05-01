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
var screen = blessed.screen()

var grid = new contrib.grid({rows: 12, cols: 12, screen: screen})

//var gauge = grid.set(1, 1, 2, 4, contrib.gauge, {label: 'Progress', stroke: 'green', fill: 'white'})
 var gauge = grid.set(0, 0, 4, 12, contrib.gauge,
    { label: 'Progress'
    , stroke: 'green'
    , fill: 'white'});
screen.append(gauge);

gauge.setPercent(0)

var log = grid.set(4, 1, 4, 10, contrib.log,
  { fg: "green"
  , selectedFg: "green"
  , label: 'Status'});

screen.append(log);



screen.key(['escape', 'q', 'C-c'], function(ch, key) {
    return process.exit(0);
});

screen.on('resize', function() {
  gauge.emit('attach');
  log.emit('attach');
});

screen.render()

// global
//number of records to process/thread
var batchsize = 100;

//number of records to process
var total = 0;

//number of records proccessed
var count = 0;

//format source patient record and import
var formatObservation = function(record) {
    var givenName = randomname({
        random: Math.random,
        first: true,
        gender: gender.toLowerCase()
    })
    var patient = {
        "person": person,
        "identifiers":identifiers,
    }
    return patient;


}


var importBatch = function(group) {
    var pp = Q.defer();
    var sql = 'SELECT * from Patients where PatientID not in (select PatientID from OpenMRSPatients)';
    if(group) {
        sql+=' and RIGHT(MD5(PatientId),1) = "'+group+'"';
    }
    sql+=' limit ' + batchsize + ';';
    db.query(sql).then(
        function handleSettled(snapshots) {
            if (snapshots[0].length > 0) {
                var promises = snapshots[0].map(
                    function iterator(row) {
                        var patient = formatPatient(row);
                        var options = {
                            method: 'POST',
                            uri: config.url,
                            headers: {
                                "Authorization": config.auth
                            },
                            json: patient // Automatically stringifies the body to JSON
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

            Q.allSettled(promises)
                .then(
                    function handleSettled(sn) {




                        var promises2 = sn.map(
                            function iterator(r) {

                                if (r.value) {
                                    var record = r.value;
                                    var UUID = record.uuid;
                                    var PatientID = record.PatientID;
                                    var FirstName = record.person.names[0].givenName;
                                    var LastName = record.person.names[0].familyName;
                                    var sql = 'insert into OpenMRSPatients (`UUID`,`PatientID`,`FirstName`,`LastName`) values ("' + UUID + '","' + PatientID + '","' + FirstName + '","' + LastName + '");';
                                    var promise = db
                                        .query(sql)
                                        .then(
                                            function handleResponse(results) {
                                                return 1;

                                            },
                                            function handleError(error) {
                                                return 0
                                            }
                                        );
                                    return promise;
                                } else {
                                    return (Q.reject(r.value));
                                }
                            })

                        Q.allSettled(promises2)
                            .then(
                                function handleSettled(sn2) {

                                    cc = 0;
                                    for (var i = 0; i < sn2.length; i++) {
                                        if (sn2[i].state == 'fulfilled') {
                                            cc = cc + sn2[i].value;
                                        }
                                    }

                                    count = count + cc;
                                    var percent = (count / total);
                                    log.log(count + " of " + total);
                                    //screen.render()
                                    gauge.setPercent(percent)
                                    importBatch(group)

                                });



                    }
                );
        }
    );
    return (pp);
}
