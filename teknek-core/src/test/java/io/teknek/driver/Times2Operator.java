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

import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import io.teknek.model.Tuple;

public class Times2Operator extends Operator {
  public void handleTuple(ITuple t) {
    ITuple tnew = new Tuple();
    tnew.setField("x", ((Integer) t.getField("x")).intValue() * 2);
    collector.emit(tnew);
  }
}
