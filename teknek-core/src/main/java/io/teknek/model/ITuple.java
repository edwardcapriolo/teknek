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

import java.util.Set;

/**
 * A tuple is a row, or a map of data. Currently the semantics of how Feeds and Operators interact
 * with tuples is left to the user. 
 * 
 * @author edward
 * 
 */
public interface ITuple {

  /**
   * 
   * @param name
   * @return True if the tuple has a tuple of the given name
   */
  public boolean hasField(String name);

  /**
   * 
   * @param name
   *          the name of the field
   * @param value
   *          the value of the field
   */
  public void setField(String name, Object value);

  /**
   * 
   * @param name
   *          the name of the field
   * @return the value of the field or null if not found
   */
  public Object getField(String name);

  /**
   * Set a field in this tuple and return an instance of this
   * 
   * @param name
   *          the field to set
   * @param value
   *          the value of the field
   * @return returns instance
   */
  public ITuple withField(String name, Object value);

  /**
   * Clears the current tuple of all fields
   */
  public void clearFields();

  /**
   * 
   * @return Set of all fields contained in the tuple
   */
  public Set<String> listFields();
}
