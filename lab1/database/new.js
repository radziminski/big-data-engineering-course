//”cassandra-driver” is in the node_modules folder. Redirect if necessary.



//var cassandra = require('cassandra-driver'); 
//Replace Username and Password with your cluster settings
//var authProvider = new cassandra.auth.PlainTextAuthProvider('Username', 'Password');

//var client = new cassandra.Client({contactPoints: contactPoints, authProvider: authProvider, keyspace:'grocery'});
//var client = new cassandra.Client({contactPoints: ['localhost'], localDataCenter: 'datacenter1', keyspace:'xxx'});

const columns = `video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description,region`;
const columnsWithTypes = `video_id text,trending_date date,title text,channel_title text,category_id text,publish_time timestamp,tags list<text>,views int,likes int,dislikes int,comment_count int,thumbnail_link text,comments_disabled boolean,ratings_disabled boolean,video_error_or_removed boolean,description text,region text`;
importCsv('./data/output.csv', null, 'videos', columns);
// CREATE KEYSPACE xxx WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':3}

createTable(client, 'videos', columnsWithTypes, ['video_id']);
importCsv('./data/output.csv', client, 'videos', columns);
// COPY test(video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description) FROM 'output' WITH DELIMITER='${delimiter}' AND HEADER=TRUE AND QUOTE='${quote}'`;

// Get all trending videos for a given channel_title
//createTable(client, 'videos_by_channel_title', columnsWithTypes, ['channel_title']);
// importCsv('./data/CAformatted.csv', client, 'videos_by_channel_title', columns);

// Get all trending videos for a given region and trending-date (should allow range queries (from-to) for the trending-date) ordered by likes
// createTable(client, 'videos_by_region_and_date', columnsWithTypes, ['region'], ['trending_date', 'likes'], [{name: 'trending_date', type: 'ASC'}, {name: 'likes', type: 'ASC'}]);
//importCsv('./data/output.csv', client, 'videos', columns);

// Get all trending videos for a given region and category_id containing the tag “comedy”
// createTable(client, 'videos_by_region_and_category', columnsWithTypes, ['region, category_id']);
// importCsv('./data/output3.csv', client, 'videos_by_region_and_category', columns);
// Execute: CREATE INDEX tag_id ON videos_by_region_and_category(tags);

// Try some other queries like you would in an RDBMS
// Get all trending videos for publish_time and video_error_or_removed
// createTable(client, 'videos_by_publish_time_and_video_error', columnsWithTypes, ['publish_time, video_error_or_removed']);
// importCsv('./data/output3.csv', client, 'videos_by_publish_time_and_video_error', columns);

// Get all trending videos with disabled comments ordered by dislikes 
// createTable(client, 'videos_by_comments_order_by_dislikes', columnsWithTypes, ['comments_disabled'], ['dislikes']);
// importCsv('./data/output3.csv', client, 'videos_by_comments_order_by_dislikes', columns);

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
  FROM '${fileName}' WITH DELIMITER='${delimiter}' AND HEADER=TRUE AND QUOTE='${quote}' AND CHUNKSIZE=50 AND NUMPROCESSES=4 AND MAXATTEMPTS=0`;
  console.log(query);
  client.execute(query, [], (err, result) => {
    if(err) {
    console.log('execute failed: ${err}', err);
    } else {
    console.log(result.columns);
    process.exit(0);
    }
  });
  
}