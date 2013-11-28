package io.teknek.plan;

public class Plan {

  private String name;
  private FeedDesc feedDesc;
  private OperatorDesc rootOperator;
  private OffsetStorageDesc offsetStorageDesc;
  private boolean disabled;
  private int maxWorkers;
  private int tupleRetry;
  private int offsetCommitInterval;
 
  public Plan(){
  }
  
  public FeedDesc getFeedDesc() {
    return feedDesc;
  }
  
  public void setFeedDesc(FeedDesc feedDesc) {
    this.feedDesc = feedDesc;
  }
  
  public OperatorDesc getRootOperator() {
    return rootOperator;
  }
  
  public void setRootOperator(OperatorDesc rootOperator) {
    this.rootOperator = rootOperator;
  }
  
  public Plan withFeedDesc(FeedDesc feedDesc) {
    this.feedDesc = feedDesc;
    return this;
  }
  
  public Plan withRootOperator(OperatorDesc rootOperator) {
    this.rootOperator = rootOperator;
    return this;
  }
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }

  public boolean isDisabled() {
    return disabled;
  }

  public void setDisabled(boolean disabled) {
    this.disabled = disabled;
  }

  public int getMaxWorkers() {
    return maxWorkers;
  }

  public void setMaxWorkers(int maxWorkers) {
    this.maxWorkers = maxWorkers;
  }

  public OffsetStorageDesc getOffsetStorageDesc() {
    return offsetStorageDesc;
  }

  public void setOffsetStorageDesc(OffsetStorageDesc offsetStorageDesc) {
    this.offsetStorageDesc = offsetStorageDesc;
  }
  
  public Plan withOffsetStorageDesc(OffsetStorageDesc desc){
    setOffsetStorageDesc(desc);
    return this;
  }
  

  public int getTupleRetry() {
    return tupleRetry;
  }

  public void setTupleRetry(int tupleRetry) {
    this.tupleRetry = tupleRetry;
  }
  
  public Plan withTupleRetry(int tupleRetry) {
    setTupleRetry(tupleRetry);
    return this;
  }
  
  public int getOffsetCommitInterval() {
    return offsetCommitInterval;
  }

  public void setOffsetCommitInterval(int offsetCommitInterval) {
    this.offsetCommitInterval = offsetCommitInterval;
  }

  public Plan withOffsetCommitInterval(int offsetCommitInterval) {
    this.offsetCommitInterval = offsetCommitInterval;
    return this;
  }
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
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
    Plan other = (Plan) obj;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    return true;
  }

}