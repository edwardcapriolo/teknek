/*
Copyright 2013 Edward Capriolo, Matt Landolf, Lodwin Cueto

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package io.teknek.plan;

import io.teknek.model.Operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OperatorDesc {

  private String spec;
  private String script;
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
  
  
}
