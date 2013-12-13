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
public abstract class DynamicInstantiatable {

  /**
   * the spec this field is used to chose the evaluation engine for the dynamic code. 
   */
  protected String spec;
  
  /**
   * If a script is specified it is typically inline code in the form of a string 
   */
  protected String script;
  
  /**
   * Typically the fully qualified name of a a java class
   */
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
