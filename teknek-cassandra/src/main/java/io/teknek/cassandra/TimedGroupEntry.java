package io.teknek.cassandra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class TimedGroupEntry {
  private long eventTimeInMillis;
  private Map<String,String> eventProperties;
  //private String eventName;
  private long incrementValue;
  private List<List<String>> groups;
  
  public TimedGroupEntry(){
    groups = new ArrayList<List<String>>();
    eventProperties = new HashMap<>();
  }

  public long getEventTimeInMillis() {
    return eventTimeInMillis;
  }

  public void setEventTimeInMillis(long eventTimeInMillis) {
    this.eventTimeInMillis = eventTimeInMillis;
  }

  public Map<String, String> getEventProperties() {
    return eventProperties;
  }

  public void setEventProperties(Map<String, String> eventProperties) {
    this.eventProperties = eventProperties;
  }

  /*
  public String getEventName() {
    return eventName;
  }

  public void setEventName(String eventName) {
    this.eventName = eventName;
  }
    */

  public long getIncrementValue() {
    return incrementValue;
  }

  public void setIncrementValue(long incrementValue) {
    this.incrementValue = incrementValue;
  }


  public List<List<String>> getGroups() {
    return groups;
  }

  public void setGroups(List<List<String>> groups) {
    this.groups = groups;
  }
  
}