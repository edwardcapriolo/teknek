technique
=========

Stream Processing Platform.

Terminology
-----

Feed - An abstraction for an input source. A feed produces tuples. A feed can be a message queue, new rows in a relational database, or anything else you may want to process.

Tuple - a map that contains 0 or more key value pairs

Operator - An abstraction that takes a tuple as input and optionally produces a tuple as output.

Plan - Connects a feed to a series of operators . Data moves from the feed while being transformed be the operators.

Components
----
Zookeeper - Zookeeper is a distributed coordination service. Technique uses zookeeper to store Plan's as well as the state of worker nodes

Kafka - Kafka is a distributed message queue. Technique uses zookeeper to shuffle data between process nodes. It can use Kafka as a source of data.

Technique-core(worker) - This component runs on a cluster of machine executing the work described by the plan

Technique-web - This component provides the front end to configure and manage Technique. From the interface you can create and modify plans and debug the output and input from feeds and operators.

Notes on query language
-----

A simple topology read from kafka, send data to operator 
that adds 2, then operator that times5. Then sends to outtopic

So if produces 5.
Operator one is 5 + 2 emits 7
Operator two 7 * 5 emits 35
Operator three puts 35 on result topic 

    technique> CREATE PLAN myPlan;
    myPlan> CREATE FEED myFeed using 'technique.kafka.feed';
    myFeed> SET PROPERTY 'topic' AS 'firehoze';
    myFeed> SET PROPERTY 'broker.list' AS 'node1:9999';
    myFeed> EXIT;
    myPlan> CREATE OPERATOR plus2 AS 'technique.samples.Plus2';
    plus2> SET PROPERTY 'src.tuple' AS 'columnX';
    plus2> EXIT;
    myPlan> CREATE OPERATOR times5 AS 'technique.samples.times5';
    minus1> SET PROPERTY 'src.tuple' AS 'columnX';
    minus1> EXIT;
    myPlan> CREATE OPERATOR outtopic AS 'technique.kafka.TopicOut';
    outtopic> SET PROPERTY 'topic' AS 'outputtopic';
    outtopic> EXIT; 
    myPlan> SET ROOT 'plus2';
    myPlan> FOR 'plus2' ADD CHILD 'times5'; 
    myPlan> FOR 'times5 ADD CHILD 'outtopic';
    technique> COPY RUN START; #save settings cisco style

We should also be able to define operators in the CLI (on the fly)

    technique> DEFINE OPERATOR Times5;
    operator-Times5> 
        public void handleTuple(Tuple t) {
          Tuple result = new Tuple();
          result.setField("x", ((Integer) t.getField("x")).intValue() * result);
          collector.emit(result);
        }
    ^D
    Operator compiled successfully
    technique> 
  
