package io.teknek.plan;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;

import io.teknek.driver.TestDriverFactory;

public class BundleGenerator {
  public static void main(String [] args) throws JsonGenerationException, JsonMappingException, IOException{
    Bundle b = new Bundle();
    b.setBundleName("itests");
    b.setPackageName("io.teknek");
    b.getFeedDescList().add(TestDriverFactory.buildPureGroovyFeedDesc());
    b.getOperatorList().add(TestDriverFactory.getIdentityGroovyOperator());
    ObjectMapper om = new ObjectMapper();
    om.getSerializationConfig().set(Feature.INDENT_OUTPUT, true);
    System.out.println( om.writeValueAsString(b) );
  }
}
