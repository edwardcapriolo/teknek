package technique.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import technique.model.Operator;


public class OperatorDesc {

  private String operatorClass;

  private Map<String,Object> parameters;
  
  private List<OperatorDesc> children;
  
  public OperatorDesc(){
    children = new ArrayList<OperatorDesc>();
  }
  
  public OperatorDesc(Operator o){
    this();
    this.operatorClass = o.getClass().getName();
  }
  
  public String getOperatorClass() {
    return operatorClass;
  }
  public void setOperatorClass(String operatorClass) {
    this.operatorClass = operatorClass;
  }
  public Map<String,Object> getParameters() {
    return parameters;
  }
  public void setParameters(Map<String,Object> parameters) {
    this.parameters = parameters;
  }
  
  public OperatorDesc withOperatorClass(String operatorClass) {
    this.operatorClass = operatorClass;
    return this;
  }
  
  public OperatorDesc withParameters(Map<String,Object> parameters) {
    this.parameters = parameters;
    return this;
  }

  public List<OperatorDesc> getChildren() {
    return children;
  }

  public void setChildren(List<OperatorDesc> children) {
    this.children = children;
  }
  
  public OperatorDesc withNextOperator(OperatorDesc d){
    children.add(d);
    return this;
  }
}
