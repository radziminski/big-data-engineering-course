//”cassandra-driver” is in the node_modules folder. Redirect if necessary.
var cassandra = require('cassandra-driver'); 
//Replace Username and Password with your cluster settings
//var authProvider = new cassandra.auth.PlainTextAuthProvider('Username', 'Password');

//var client = new cassandra.Client({contactPoints: contactPoints, authProvider: authProvider, keyspace:'grocery'});
var client = new cassandra.Client({contactPoints: ['localhost'], localDataCenter: 'datacenter1', keyspace:'system'});

//Execute the queries 
var query = 'SELECT cluster_name, listen_address FROM local;';
console.log("Executeing: " + query);
client.execute(query, [], (err, result) => {
  if(err) {
	console.log('execute failed: ${err}', err);
  } else {
	console.log(result.columns);
	process.exit(0);
  }
});