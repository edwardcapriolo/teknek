package technique.plan;

public class Plan {

  private String name;
  private FeedDesc feedDesc;
  private OperatorDesc rootOperator;
  private boolean disabled;
 
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

  
}

