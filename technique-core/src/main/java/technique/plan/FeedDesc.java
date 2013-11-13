package technique.plan;

import java.util.Map;

public class FeedDesc {
  private String feedClass;
  @SuppressWarnings("rawtypes")
  private Map properties;
  
  public FeedDesc(){
    
  }

  public String getFeedClass() {
    return feedClass;
  }

  public void setFeedClass(String feedClass) {
    this.feedClass = feedClass;
  }

  public Map getProperties() {
    return properties;
  }

  public void setProperties(Map properties) {
    this.properties = properties;
  }
  
  public FeedDesc withFeedClass(String feedClass) {
    this.feedClass = feedClass;
    return this;
  }
  
  public FeedDesc withProperties(Map properties) {
    this.properties = properties;
    return this;
  }

  
}
