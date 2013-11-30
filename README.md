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

Tuple - a map that contains 0 or more key value pairs

Operator - An abstraction that takes a tuple as input and optionally produces a tuple as output.

Plan - Connects a feed to a series of operators . Data moves from the feed while being transformed be the operators.

Components
----
Zookeeper - Zookeeper is a distributed coordination service. Teknek uses zookeeper to store Plan's as well as the state of worker nodes

Teknek-core(worker) - This component runs on a cluster of machine executing the work described by the plan

Teknek-web - This component provides the front end to configure and manage Teknek. From the interface you can create and modify plans and debug the output and input from feeds and operators.

Teknek integrations
----

Teknek is desigigned so that it can integrated with software currently in your software stack. To do this its core is designed to be agnostic and plugable. For example, a Feed could be constructed over the Kafka message queue system, after processing the results can be written to a NoSQL database like Cassandra. However data can just as easily be read from a MySQL database and written to HBase by swapping components in the plan.

Teknek provides several out-of-the-box implementations of Feeds an Operators to help users get up and running quickly. 

Cassandra - Cassandra is a nosql data store. This package includes operators to write data and increment counters.
Kafka - Kafka is a distributed message queue. Teknek can use kafka's partitioning ability to easily achive group-by semantics. Kafka support includes Feed and output operator.


Notes on the domain specifc language (PIL plan integration language?)
-----

A simple topology read from kafka, send data to operator 
that adds 2, then operator that times5. Then sends to outtopic

So if produces 5.
Operator one is 5 + 2 emits 7
Operator two 7 * 5 emits 35
Operator three puts 35 on result topic 

    teknek> CREATE PLAN myPlan;
    myPlan> CREATE FEED myFeed using 'teknek.kafka.feed';
    myFeed> SET PROPERTY 'topic' AS 'firehoze';
    myFeed> SET PROPERTY 'broker.list' AS 'node1:9999';
    myFeed> EXIT;
    myPlan> CREATE OPERATOR plus2 AS 'teknek.samples.Plus2';
    plus2> SET PROPERTY 'src.tuple' AS 'columnX';
    plus2> EXIT;
    myPlan> CREATE OPERATOR times5 AS 'teknek.samples.times5';
    minus1> SET PROPERTY 'src.tuple' AS 'columnX';
    minus1> EXIT;
    myPlan> CREATE OPERATOR outtopic AS 'teknek.kafka.TopicOut';
    outtopic> SET PROPERTY 'topic' AS 'outputtopic';
    outtopic> EXIT; 
    myPlan> SET ROOT 'plus2';
    myPlan> FOR 'plus2' ADD CHILD 'times5'; 
    myPlan> FOR 'times5 ADD CHILD 'outtopic';
    teknek> COPY RUN START; #save settings cisco style

We should also be able to define operators in the CLI (on the fly)

    teknek> DEFINE OPERATOR Times5;
    operator-Times5> 
        public void handleTuple(Tuple t) {
          Tuple result = new Tuple();
          result.setField("x", ((Integer) t.getField("x")).intValue() * result);
          collector.emit(result);
        }
    ^D
    Operator compiled successfully
    teknek> 
  
