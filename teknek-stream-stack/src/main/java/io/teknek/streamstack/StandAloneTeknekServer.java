package io.teknek.streamstack;

import io.teknek.daemon.TeknekDaemon;

import java.util.Properties;

public class StandAloneTeknekServer {
  TeknekDaemon td;

  public void start() {
    Properties props = new Properties();
    props.put(TeknekDaemon.ZK_SERVER_LIST, "localhost:2181");
    td = new TeknekDaemon(props);
    td.init();
  }
  
  public static void main (String [] args){
    StandAloneTeknekServer d = new StandAloneTeknekServer();
    d.start();
  }
}
