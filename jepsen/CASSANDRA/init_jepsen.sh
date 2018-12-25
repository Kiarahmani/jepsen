cd ~/Jepsen_Java_Tests/
git pull
cd -

# files necessary to restore the initial db state
docker cp ~/snapshots/seats jepsen-n1:/root/
docker cp ~/snapshots/seats jepsen-control:/root/
echo 'Initial snapshots moved to jepsen-n* node'





# file containing the table names
docker cp ~/Jepsen_Java_Tests/table.names jepsen-control:/root/
echo 'table.names file moved to jepsen-control node'


# copy the java application to the jepsen control node
docker cp ~/Jepsen_Java_Tests/src/main/java/. jepsen-control:/root/java
echo 'Java application copied'

# copy .cql file to jepsen-n1 for keyspace intialization
docker cp ~/Jepsen_Java_Tests/ddl.cql jepsen-n1:/root/
echo '.cql file transfered'

# copy cassandra jdbc wrapper .jar into jepsen control node
docker cp /home/ubuntu/jepsen/jepsen/CASSANDRA/cassandra-jdbc-wrapper-3.1.0-SNAPSHOT.jar jepsen-control:/jepsen/CASSANDRA/jepsen.cassandra/resources/
echo 'Cassandra JDBC wrapper copied'
