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
package io.teknek.feed;

import io.teknek.model.ITuple;

public abstract class FeedPartition {

  /**
   * Reference to the parent of this partition
   */
  protected Feed feed;
  
  /**
   * This field uniquely identifies a partition of a feed. In could be critical
   * in cases where you wish client to re-bind to prospective feeds. 
   */
  private String partitionId;
  
  public FeedPartition(Feed feed, String partitionId){
    this.feed = feed;
    this.partitionId = partitionId;
  }
  
  public abstract void initialize();
  /**
   * Read the next value from the feed into the tupleRef passed in.
   * Typically class always returns true and blocks on next read until new
   * data appears unless the user wishes to construct a feed that ends
   * 
   * @param tupleRef
   * @return true if the partition contains more tuples or might contain more tuples in the future 
   */
  public abstract boolean next(ITuple tupleRef);
  
  public abstract void close();

  public String getPartitionId() {
    return partitionId;
  }

  public Feed getFeed() {
    return feed;
  }
  
  /**
   * 
   * @return true if both getOffset and setOffset are supported
   */
  public abstract boolean supportsOffsetManagement();
  
  /**
   * A string that represents the current offset of the feed. The format 
   * is not a general one, each FeedPartition persists strings only meant
   * to be read back by the same class
   * @return a string which represents the current offset
   */
  public abstract String getOffset();
  
  /**
   * Called only once to advance the feed to a specific starting point.
   * @param offset
   */
  public abstract void setOffset(String offset);
  
}