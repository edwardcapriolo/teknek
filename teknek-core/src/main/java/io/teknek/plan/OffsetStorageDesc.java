package io.teknek.plan;

import java.util.Map;

public class OffsetStorageDesc {

  private String operatorClass;
  private Map<String,Object> parameters;
  
  public OffsetStorageDesc(){
    
  }
  
  public OffsetStorageDesc(String operatorClass, Map<String,Object> parameters){
    setOperatorClass(operatorClass);
    setParameters(parameters);
  }

  public String getOperatorClass() {
    return operatorClass;
  }

  public void setOperatorClass(String operatorClass) {
    this.operatorClass = operatorClass;
  }

  public Map<String, Object> getParameters() {
    return parameters;
  }

  public void setParameters(Map<String, Object> parameters) {
    this.parameters = parameters;
  }
  
  public OffsetStorageDesc withParameters(Map<String, Object> parameters){
    setParameters(parameters);
    return this;
  }
  
  public OffsetStorageDesc withOperatorClass(String className){
    setOperatorClass(className);
    return this;
  }
}
