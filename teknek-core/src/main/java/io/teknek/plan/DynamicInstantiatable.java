package io.teknek.plan;

/**
 * A class which encapsulates the attributes to dynamically load another class. Based on the spec 
 * the script and theClass properties may be optional. 
 * 
 * When the spec is null or java a java class assumed to be in the classpath of teknek daemon is used
 * 
 * @author edward
 *
 */
public class DynamicInstantiatable {

  protected String spec;
  protected String script;
  protected String theClass;
  
  public String getSpec() {
    return spec;
  }

  public void setSpec(String spec) {
    this.spec = spec;
  }

  public String getScript() {
    return script;
  }

  public void setScript(String script) {
    this.script = script;
  }

  public String getTheClass() {
    return theClass;
  }

  public void setTheClass(String theClass) {
    this.theClass = theClass;
  }
  
}
