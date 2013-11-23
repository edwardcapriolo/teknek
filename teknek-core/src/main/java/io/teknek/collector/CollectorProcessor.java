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
package io.teknek.collector;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import io.teknek.model.Tuple;

import java.util.ArrayList;
import java.util.List;


public class CollectorProcessor implements Runnable {
  Collector collector;
  List<Operator> children;
  boolean goOn = true;
  
  public CollectorProcessor(){
    children = new ArrayList<Operator>();
    collector = new Collector();
  }
  
  public void run(){
    while(goOn){
      try {
        ITuple tuple = collector.take();
        for (Operator o: children){
          int attemptCount = 0;
          boolean complete = false;
          while (attemptCount++ < 4 && complete == false){
            try {
              o.handleTuple(tuple);
              complete = true;
            } catch (RuntimeException ex){
              //ex.printStackTrace();
            }
          }
        }
      } catch (InterruptedException e) {       
        e.printStackTrace();
      }
    }
  }

  public Collector getCollector() {
    return collector;
  }

  public List<Operator> getChildren() {
    return children;
  }
  
  
  
}
