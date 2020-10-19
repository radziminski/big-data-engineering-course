//Creating a new connection pool to multiple hosts.
var cql = require('node-cassandra-cql');
var client = new cql.Client({hosts: ['localhost']});
client.execute('create table test (a in primary key, b text)',
  function(err, result) {
    if (err) console.log('execute failed: ${err}', err);
    else console.log(result.rows);
  }
);
