package io.teknek.streamstack;

import io.teknek.daemon.Starter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public class PropertiesLoader {
  
  final static Logger LOGGER = Logger.getLogger(Starter.class.getName());

  public static Properties getProps() {
    Properties props = new Properties();
    try (InputStream inputStream = StandAloneZooKeeperServer.class.getClassLoader()
            .getResourceAsStream("streamstack.properties");) {
      if (inputStream != null) {
        props.load(inputStream);
      }
    } catch (IOException e) {
      LOGGER.debug("loading props file " + e);
    }
    return props;
  }
}
