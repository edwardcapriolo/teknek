package io.teknek.plan;

import java.util.ArrayList;
import java.util.List;

public class Bundle {
  private String packageName;

  @Deprecated
  private String bundleName;

  private List<FeedDesc> feedDescList;

  private List<OperatorDesc> operatorList;

  public Bundle() {
    feedDescList = new ArrayList<>();
    operatorList = new ArrayList<>();
  }

  public String getPackageName() {
    return packageName;
  }

  public void setPackageName(String packageName) {
    this.packageName = packageName;
  }

  public String getBundleName() {
    return bundleName;
  }

  public void setBundleName(String bundleName) {
    this.bundleName = bundleName;
  }

  public List<FeedDesc> getFeedDescList() {
    return feedDescList;
  }

  public void setFeedDescList(List<FeedDesc> feedDescList) {
    this.feedDescList = feedDescList;
  }

  public List<OperatorDesc> getOperatorList() {
    return operatorList;
  }

  public void setOperatorList(List<OperatorDesc> operatorList) {
    this.operatorList = operatorList;
  }
  
}
