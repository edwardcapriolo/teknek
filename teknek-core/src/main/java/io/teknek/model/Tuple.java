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
package io.teknek.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
 
public class Tuple implements ITuple {
  private Map<String, Object> columns;
  
  public Tuple(){
    columns = new HashMap<String,Object>();
  }
  
  public void setField(String name, Object value){
    columns.put(name, value);
  }
  public Object getField(String name){
    return columns.get(name);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((columns == null) ? 0 : columns.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Tuple other = (Tuple) obj;
    
    if (columns == null) {
      if (other.columns != null)
        return false;
    } else if (!columns.equals(other.columns))
      return false;
    return true;
  }
  
  public String toString(){
    return this.columns.toString();
  }

  @Override
  public ITuple withField(String name, Object value) {
    setField(name, value);
    return this;
  }

  @Override
  public boolean hasField(String name) {
    return columns.containsKey(name);
  }

  @Override
  public void clearFields() {
    columns.clear();
  }

  @Override
  public Set<String> listFields() {
    return columns.keySet();
  }
}
