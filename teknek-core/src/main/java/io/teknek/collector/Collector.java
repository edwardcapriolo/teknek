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

import io.teknek.model.ICollector;
import io.teknek.model.ITuple;
import io.teknek.model.Tuple;

import java.util.concurrent.ArrayBlockingQueue;

 
/**
 * Positioned between two operators. emit take and peek work
 * on an underlying blocking queue which should offer flow control.
 *
 */
public class Collector extends ICollector {

  private ArrayBlockingQueue<ITuple> collected;

  public Collector(){
    collected = new ArrayBlockingQueue<ITuple>(4000);
  }
  
  @Override
  public void emit(ITuple out) {
    collected.add(out);
  }

  public ITuple take() throws InterruptedException{
    return collected.take();
  }
  
  public ITuple peek() throws InterruptedException{
    return collected.peek();
  }
  
  /** returns size of the queue managed by this object */
  public int size() {
    return collected.size();
  }
}
