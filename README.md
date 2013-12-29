teknek (tekˈnēk)
=========

Stream Processing Platform

tech·nique
/tekˈnēk/
noun

a way of carrying out a particular task, esp. the execution or performance of an artistic work or a scientific procedure.

Notes
-----
We are still putting the initial cut of the software together.  Stay tuned.

Terminology
-----

Feed - An abstraction for an input source. A feed produces tuples. A feed can be a message queue, new rows in a relational database, or anything else you may want to process.

Tuple - a map that contains 0 or more key value pairs, could be thought of a row.

Operator - An abstraction that takes a tuple as input and optionally produces a tuple as output.

Plan - Connects a feed to a series of operators . Data moves from the feed while being transformed be the operators.

Components
----
Zookeeper - Zookeeper is a distributed coordination service. Teknek uses zookeeper to store Plan's as well as the state of worker nodes.

Teknek-core(worker) - This component runs on a cluster of machine executing the work described by the plan

Teknek-web - This component provides the front end to configure and manage Teknek. From the interface you can create and modify plans and debug the output and input from feeds and operators.

Build Instructions
----
Currently the project is organized as several separate maven projects with dependencies. While using a maven multiple-module project may not be out of the question, for now it seems better to serparate core functionality from plugable integrations. By keeping the classpath thin we can potentially avoid
complicated classloader issues down the road.

Run these commands:

    git clone <this github>
    sh build.sh


Teknek integrations
----

Teknek is desigigned so that it can integrated with software currently in your software stack. To do this its core is designed to be agnostic and plugable. For example, a Feed could be constructed over the Kafka message queue system, after processing the results can be written to a NoSQL database like Cassandra. However data can just as easily be read from a MySQL database and written to HBase by swapping components in the plan.

Teknek provides several out-of-the-box implementations of Feeds an Operators to help users get up and running quickly. 

teknek-cassandra - Cassandra is a nosql data store. This package includes operators to write data and increment counters.
teknek-kafka - Kafka is a distributed message queue. Teknek can use kafka's partitioning ability to easily achive group-by semantics. Kafka support includes Feed and output operator.

Stream Operator Language (SOL)
-----

The goal of SOL is to provide a language to build a Plan by linking Feeds to a tree of Operators. This language allows the user to wire together components without using an IDE. Not only can users wire together compoenents, they can also write code inline for rapid prototyping. 

    teknek> create plan k
    plan> configure feed k
    feed> set class as io.teknek.kafka.SimpleKafkaFeed
    feed> set feedspec as url
    feed> set script as http://localhost:8080//teknek-web/Serve/teknek-kafka-0.0.1-SNAPSHOT-jar-with-dependencies.jar
    feed> set property simple.kafka.feed.consumer.group as 'group1'
    feed> set property simple.kafka.feed.reset.offset as 'yes'
    feed> set property simple.kafka.feed.partitions as 1
    feed> set property simple.kafka.feed.topic as 'clickstream'
    feed> set property simple.kafka.feed.zookeeper.connect as 'localhost:2181'
    feed> exit
    plan> import https://raw.github.com/edwardcapriolo/teknek/master/teknek-core/src/test/resources/bundle_io.teknek_itests1.0.0.json
    plan> load io.teknek groovy_identity operator as groovy_identity
    operator> exit
    plan> set root groovy_identity
    plan> set maxworkers 1
    plan> set tupleRetry 1
    plan> set offsetCommitInterval 1
    plan> save


