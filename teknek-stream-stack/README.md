Starts the stream-stack KZCT (Kafka Zookeeper Cassandra Teknek) making it easy to play with teknek


cassandra port 9157
zookeeper port 2181
 
mvn clean install assembly:single

    cd target/teknek-stream-stack-0.0.1-SNAPSHOT/teknek-stream-stack-0.0.1-SNAPSHOT/

or extract tar

    target/teknek-stream-stack-0.0.1-SNAPSHOT.tar.gz


[edward@jackintosh teknek-stream-stack]$ cat /tmp/streamstack.properties 

    starter.embeddedzookeeper=true
    starter.embeddedzookeeper.log=./target/zk_log
    starter.embeddedzookeeper.snap=./target/zk_log
    starter.embeddedkafka=true
    starter.embeddedkafka.log=./target/kflog


    [edward@jackintosh teknek-stream-stack-0.0.1-SNAPSHOT]$ sh start-all.sh 
    DEBUG 01:27:02,122 getResourceAsStream failed to load properties
    INFO 01:27:02,137 Starting embedded zookeeper
    INFO 01:27:02,137 This is only appropriate for single node deployments
    DEBUG 01:27:02,352 getResourceAsStream failed to load properties
    INFO 01:27:02,361 starting single node zookeeper server on localhost 2181
    DEBUG 01:27:02,593 my UUID56a84114-97e1-449f-a17d-1f02e8937c07
    connecting to localhost:2181
    WARN 01:27:02,846 Exception causing close of session 0x0 due to java.io.IOException: ZooKeeperServer not running
    WARN 01:27:03,140 Property enable.zookeeper is not valid
    ERROR 01:27:03,741 Unable to initialize MemoryMeter (jamm not specified as javaagent).  This means Cassandra will be unable to measure object sizes accurately and may consequently OOM.
    WARN 01:27:04,209 MemoryMeter uninitialized (jamm not specified as java agent); KeyCache size in JVM Heap will not be calculated accurately. Usually this means cassandra-env.sh disabled jamm because you are using a buggy JRE; upgrade to the Sun JRE instead
     WARN 01:27:04,923 No host ID found, created 756188f1-41a5-41e6-bdc5-4240357f1ff2 (Note: This should happen exactly once per node).
    INFO 01:27:04,963 Creating /teknek heirarchy
    DEBUG 01:27:05,025 Children found in zk[]
    WARN 01:27:05,177 Generated random token [-2432490598404475305]. Random tokens will result in an unbalanced ring; see http://wiki.apache.org/cassandra/Operations
    DEBUG 01:27:10,027 Children found in zk[]
    DEBUG 01:27:15,028 Children found in zk[]
    DEBUG 01:27:20,029 Children found in zk[]

