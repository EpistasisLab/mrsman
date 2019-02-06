#!/usr/bin/env node
'use strict';
var http = require('http'),
    httpProxy = require('http-proxy');
var proxy = httpProxy.createProxyServer({});
proxy.on('proxyReq', function(proxyReq, req, res, options) {
    proxyReq.setHeader('X-Special-Proxy-Header', 'foobar');
});
var server = http.createServer(function(req, res) {
    proxy.web(req, res, {
        target: 'http://127.0.0.1:8080'
    });
});
console.log("listening on port 5080")
server.listen(5080);
