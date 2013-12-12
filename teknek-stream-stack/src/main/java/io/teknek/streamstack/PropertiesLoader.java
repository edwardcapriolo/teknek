package io.teknek.streamstack;

import io.teknek.daemon.Starter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public class PropertiesLoader {
  
  final static Logger LOGGER = Logger.getLogger(Starter.class.getName());

  public static final String PROP_FILE_NAME = "streamstack.properties";
  
  public static Properties getProps() {
    Properties props = new Properties();
    File fs = new File("streamstack.properties");
    try (InputStream inputStream = StandAloneZooKeeperServer.class.getClassLoader()
            .getResourceAsStream("streamstack.properties");) {
      if (inputStream != null) {
        props.load(inputStream);
      } else {
        LOGGER.debug("getResourceAsStream failed to load properties");
      }
    } catch (IOException e) {
      LOGGER.debug("loading props file " + e);
    }
    if (fs.exists()){
      try (InputStream inputStream = new FileInputStream("streamstack.properties")) {
        if (inputStream != null) {
          props.load(inputStream);
        }
      } catch (IOException e) {
        LOGGER.debug("loading props file " + e);
      }
    }
    return props;
  }
}
