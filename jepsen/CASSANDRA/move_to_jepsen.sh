 #!/bin/bash
dc=" "
delim=" "
for i in `seq 1 $1`;
  do
#    echo "copying files to jepsen-n${i}"
#    docker cp ~/dev/eclipse_workspace/JepsenApplication/target/classes/. "jepsen-n${i}:/root/java"
#    docker cp /Users/Kiarash/dev/eclipse_workspace/CassandraApplication/lib/cassandra-jdbc-wrapper-3.1.0-SNAPSHOT.jar "jepsen-n${i}:/root/java"
    dc+="$delim'dc_n${i}':1"
    delim=","
  done    

# copy tha java application to the jepsen control node
docker cp ~/Jepsen_Java_Tests/src/main/java/. jepsen-control:/root/java
echo 'Java application copied'

# copy cassandra jdbc wrapper .jar into jepsen control node
docker cp /home/ubuntu/jepsen/jepsen/CASSANDRA/cassandra-jdbc-wrapper-3.1.0-SNAPSHOT.jar jepsen-control:/jepsen/CASSANDRA/jepsen.cassandra/resources/
echo 'Cassandra JDBC wrapper copied'

# drop previous keyspaces
docker exec -ti jepsen-n2 root/cassandra/bin/cqlsh $(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' jepsen-n2) -e " DROP KEYSPACE testks"
echo 'old keyspace removed'

# create a fresh keysapace
docker exec -ti jepsen-n2 root/cassandra/bin/cqlsh $(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' jepsen-n2) -e "CREATE KEYSPACE if NOT EXISTS testks WITH replication = {'class': 'NetworkTopologyStrategy', $dc}"
echo 'fresh keyspace generated'

# create a fresh table
docker exec -ti jepsen-n2 root/cassandra/bin/cqlsh $(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' jepsen-n2) -e "create TABLE testks.a (id int PRIMARY KEY , balance int)"

echo 'fresh table generated'
