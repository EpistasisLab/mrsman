#!/usr/bin/env node
'use strict';
// utility functions and modules for interfacing with and building containers
// and cloud services
var awcm = require('awcm');
var argv = require('minimist')(process.argv.slice(2));
//the maximum number of forums we can run at once
var doShared = true
var doCloud = false
var verbose = true;
var action = 'rebuild';
if (argv['_'].length > 0) {
    action = argv['_'][0];
}
if (argv['i']) {
    var ip = argv['i'];
} else {
    var ip = '127.0.0.1';
}
if (argv['c']) {
    var config = argv['c'];
} else {
    var config = 'config.json';
}


//load site data
var exP = awcm.syncFile('config.json');
exP.then(function(site) {
console.log(site);
	if (site.global.IP !== undefined) {
		ip = site.global.IP;
        }
            var forum = {};
            forum['ip'] = ip;
            forum['action'] = action;
            forum['doShared'] = doShared;
            forum['verbose'] = verbose;
            var infP = awcm.build(forum, site);
            infP.then(function(finfo) {
                if (finfo) {

                }
            }).catch(function(err) {
                console.log('error', err);
            });
    }).catch(function(err) {
        console.log('something went wrong')
        console.log(err);
        deferred.reject(new Error(err));
    })
