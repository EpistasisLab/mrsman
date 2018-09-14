var cypher = require('cypher-stream')('bolt://localhost', 'neo4j', 'password');
var count = process.argv[2] || 10000;
cypher('FOREACH (x IN range(1, '+count+') | CREATE (n:Test { name: x }))').resume();
