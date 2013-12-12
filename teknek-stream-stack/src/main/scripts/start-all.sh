CP=`ls teknek-stream-stack*.jar`
CP=$CP:streamstack.properties
for f in `ls lib/*` ; do
  CP=$CP:$f
done
/usr/java/jdk1.7.0_13/bin/java -classpath $CP io.teknek.streamstack.StandAloneZooKeeperServer &
/usr/java/jdk1.7.0_13/bin/java -classpath $CP io.teknek.streamstack.StandAloneKafkaServer &
