//”cassandra-driver” is in the node_modules folder. Redirect if necessary.
var cassandra = require('cassandra-driver'); 
//Replace Username and Password with your cluster settings
//var authProvider = new cassandra.auth.PlainTextAuthProvider('Username', 'Password');

//var client = new cassandra.Client({contactPoints: contactPoints, authProvider: authProvider, keyspace:'grocery'});
var client = new cassandra.Client({contactPoints: ['localhost'], localDataCenter: 'datacenter1', keyspace:'xxx'});

const columns = `acousticness,artists,danceability,duration_ms,energy,explicit,id,instrumentalness,key,liveness,loudness,mode,name,popularity,release_date,speechiness,tempo,valence,year`;
const columnsWithTypes = `acousticness float,artists list<text>,danceability float,duration_ms int,energy float,explicit boolean,id text,instrumentalness float,key int,liveness float,loudness float,mode int,name text,popularity int,release_date date,speechiness float,tempo float,valence float,year int`;



// createTable(client, 'spotify', columnsWithTypes, ['id']);
// importCsv('./data/data.csv', client, 'spotify', columns);
// COPY test(video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description) FROM 'output' WITH DELIMITER='${delimiter}' AND HEADER=TRUE AND QUOTE='${quote}'`;

// Get the spotify track by the name
createTable(client, 'songs_by_name', columnsWithTypes, ['name']);
importCsv('./data/data.csv', client, 'songs_by_name', columns);

// Get all spotify tracks by year
//createTable(client, 'songs_by_year', columnsWithTypes, ['year']);
//importCsv('./data/data.csv', client, 'songs_by_year', columns);

// Get all spotify tracks by key and danceability (range) sorted by energy
//createTable(client, 'songs_by_key_and_range', columnsWithTypes, ['key'], ['danceability, energy'], ['danceability']);
//importCsv('./data/data.csv', client, 'songs_by_key_and_range', columns);

client.execute('SELECT * FROM test;', [], (err, result) => {
  if(err) {
    console.log('execute failed: ${err}', err);
    } else {
    console.log(result.rows);
    process.exit(0);
    }
})

function createTable(client, name, columnsWithTypes, primaryKeys = [], clusteringKeys = [], clusteringOrderBy = []) {
  //Execute the queries 
  var query = clusteringKeys.length !== 0 
    ? `CREATE TABLE ${name}(${columnsWithTypes}, 
      PRIMARY KEY((${primaryKeys.join(', ')}), ${clusteringKeys.join(', ')}))
      ${clusteringOrderBy.length !== 0 ? ` WITH CLUSTERING ORDER BY (${clusteringOrderBy.map((el, index) => el.name + ' ' + el.type).join(', ')})` : ''}` 
    : `CREATE TABLE ${name}(${columnsWithTypes}, PRIMARY KEY((${primaryKeys.join(', ')})))`;
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
  FROM '${fileName}' WITH DELIMITER='${delimiter}' AND HEADER=TRUE AND QUOTE='${quote}' AND CHUNKSIZE=50 AND NUMPROCESSES=4`;
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