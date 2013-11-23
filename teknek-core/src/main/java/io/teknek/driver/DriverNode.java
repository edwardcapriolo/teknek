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
package io.teknek.driver;

import io.teknek.collector.CollectorProcessor;
import io.teknek.model.Operator;

import java.util.ArrayList;
import java.util.List;


public class DriverNode {

  private Operator operator;
  private CollectorProcessor collectorProcessor;
  private Thread thread ;
  private List<DriverNode> children ;
  
  public DriverNode(Operator operator, CollectorProcessor cp){
    this.operator = operator;
    this.collectorProcessor = cp;
    operator.setCollector(cp.getCollector());
    children = new ArrayList<DriverNode>();
  }
  
  /**
   * initialize driver node and all children of the node
   */
  public void initialize(){
    thread = new Thread(collectorProcessor);
    thread.start();
    for (DriverNode dn : this.children){
      dn.initialize();
    }
  }
  
  /**
   * Method adds ad child data node and bind the collect processor
   * of this node to the operator of the next node
   * @param dn
   */
  public void addChild(DriverNode dn){
    collectorProcessor.getChildren().add(dn.operator);
    this.children.add(dn);
  }
  
  public DriverNode withChild(DriverNode dn){
    addChild(dn);
    return this;
  }

  public Operator getOperator() {
    return operator;
  }

  public void setOperator(Operator operator) {
    this.operator = operator;
  }

  public CollectorProcessor getCollectorProcessor() {
    return collectorProcessor;
  }

  public void setCollectorProcessor(CollectorProcessor collectorProcessor) {
    this.collectorProcessor = collectorProcessor;
  }

  public List<DriverNode> getChildren() {
    return children;
  }
  
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append("Operator "+operator.toString());
    for (DriverNode dn : this.children){
      System.out.println(dn.toString());
    }
    return sb.toString();
  }
  
  public void prettyPrint(int tabs){
    for (int i =0 ; i< tabs;i++){
      System.out.print("--");
    }
    System.out.println(this.operator.getClass().getName());
    for ( DriverNode child : this.children){
      child.prettyPrint(tabs + 1);
    }
  }
}
