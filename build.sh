cd teknek-core
mvn clean install
mvn eclipse:eclipse
cd ..
cd teknek-cassandra
mvn clean install
mvn eclipse:eclipse
cd ..
cd teknek-hdfs
mvn clean install
mvn eclipse:eclipse
cd ..
cd teknek-kafka
mvn clean install
mvn eclipse:eclipse
mvn install assembly:single
cd ..
cd teknek-web
mvn clean install
mvn eclipse:eclipse -Dwtpversion=2.0
cd ..
cd teknek-stream-stack
mvn clean install assembly:single
mvn eclipse:eclipse
cd ..
