//”cassandra-driver” is in the node_modules folder. Redirect if necessary.
var cassandra = require('cassandra-driver'); 
//Replace Username and Password with your cluster settings
//var authProvider = new cassandra.auth.PlainTextAuthProvider('Username', 'Password');

//var client = new cassandra.Client({contactPoints: contactPoints, authProvider: authProvider, keyspace:'grocery'});
var client = new cassandra.Client({contactPoints: ['localhost'], localDataCenter: 'datacenter1', keyspace:'xxx'});

const columns = `video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description`;
const columnsWithTypes = `video_id text,trending_date date,title text,channel_title text,category_id text,publish_time timestamp,tags list<text>,views int,likes int,dislikes int,comment_count int,thumbnail_link text,comments_disabled boolean,ratings_disabled boolean,video_error_or_removed boolean,description text`;

// createTable(client, 'test', columnsWithTypes, 'video_id');
//importCsv('./data/output2.csv', client, 'test', columns);
// COPY test(video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description) FROM 'output' WITH DELIMITER='${delimiter}' AND HEADER=TRUE AND QUOTE='${quote}'`;

client.execute('SELECT * FROM test;', [], (err, result) => {
  if(err) {
    console.log('execute failed: ${err}', err);
    } else {
    console.log(result.rows);
    process.exit(0);
    }
})

function createTable(client, name, columnsWithTypes, ...primaryKeys) {
  //Execute the queries 
  var query = `CREATE TABLE ${name}(${columnsWithTypes}, PRIMARY KEY(${primaryKeys.join(', ')}))`;
  console.log("Executeing: " + query);
  client.execute(query, [], (err, result) => {
    if(err) {
    console.log('execute failed: ${err}', err);
    } else {
    console.log(result.columns);
    process.exit(0);
    }
  });
}

function importCsv(fileName, client, table, columns, delimiter = ',', quote = '"') {
  var query = `COPY ${table}(${columns})
  FROM '${fileName}' WITH DELIMITER='${delimiter}' AND HEADER=TRUE AND QUOTE='${quote}'`;
  console.log("Executeing: " + query);
  client.execute(query, [], (err, result) => {
    if(err) {
    console.log('execute failed: ${err}', err);
    } else {
    console.log(result.columns);
    process.exit(0);
    }
  });
  
}