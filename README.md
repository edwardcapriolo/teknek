technique
=========

Notes con query language
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
    myPlan> SET CHAIN 'plus2','times5', 'outtopic';
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
  
