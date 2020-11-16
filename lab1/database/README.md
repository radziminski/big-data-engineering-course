# Cassandra Cluster in Docker

This demo provides a `docker-compose` blueprint that describes a 2 node Cassandra cluster. It only exposes important Cassandra ports on the seed (main) node to the host machine. Internally, all of the nodes will be a part of the same Docker network ("cassandra-cluster") and will form a cluster using that network.

## Install & Download

- git to clone the repository
- docker & docker-compose
- latest node.js (if you'd like to work with the node application instead of CQLSH)
- Download youtube trending statistics from the elearning platform or from kaggle directly.<br>**Important:** place the files into directory `youtube-new`. The cluster will share this directory to each running container into the `/data` directory.

## Startup

In order to bring up the cluster:

- Optionally build the docker images manually first by calling `docker-compose build`
- Use `docker-compose up -d` if you want to start the services in the background.
A Cassandra seed as well as one node is started by default
- Use `docker ps` to verify the running instances (two).

Managing the cluster:

- Use `docker logs -f <container-id>` to see the logs of the respective container
- To scale up to more nodes simply type `docker-compose scale cassandra-node=<num>` where \<num\> sets the overall node count in addition to the seed node (main). One is the default value.

In order to shutdown and clean up the cluster, use `docker-compose down`

## Working with Cassandra

Next you'll find some help how to work with the running cluster.

### Run CQLSH with docker

Use docker to execute CQLSH by running

```bash
docker run -v "$PWD/youtube-new:/data" -it --network bde-cassandra-docker-cluster_cassandra-cluster --rm cassandra cqlsh cassandra-main
```

**Hint**: for Windows this needs to be executed in **Powershell**.

The network id (above: "bde-cassandra-docker-cluster_cassandra-cluster") may change on other systems, on error check

```bash
docker network ls
```

to get the right one.

### Run CQLSH locally

If you have Python 2.7 (not higher) installed you may install cqlsh locally on your
machine:

```bash
pip install cqlsh
```

After install you should be able to connect to the exposed ports of your docker machine which is usually reachable on localhost:

```bash
cqlsh localhost
```

### Work with the running nodes

To check where your keyspace is replicated you may call the *nodetool* like this:

```bash
docker exec <container-id> nodetool getendpoints <keyspace> <table> <token>
```

## Node.js application

To run the node demo install a recent version of node.js and then execute

```bash
npm install
```

to install the necessary packages/modules. Finally run `node index.js` or

```bash
npm start
```