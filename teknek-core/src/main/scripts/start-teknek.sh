CP=`ls teknek-core*.jar`
for f in `ls lib/*` ; do
  CP=$CP:$f
done
/usr/java/jdk1.7.0_13/bin/java -classpath$CP io.teknek.daemon.TeknekDaemon
