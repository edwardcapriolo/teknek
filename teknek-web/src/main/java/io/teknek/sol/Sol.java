package io.teknek.sol;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.TreeMap;

import io.teknek.datalayer.WorkerDao;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.Plan;

public class Sol {

  public static final String rootPrompt = "teknek> ";
  public static final String planPrompt = "plan> ";
  public static final String feedPrompt = "feed> ";
  
  String currentNode;
  private Plan thePlan;
  
  public Sol(){
    thePlan = new Plan();
    currentNode = rootPrompt;
  }
  
  public SolReturn send(String command){
    String [] parts = command.split("\\s+");
    if (command.equalsIgnoreCase("SHOW")){
      return new SolReturn(currentNode, new String( WorkerDao.serializePlan(thePlan)));
    }
    if (currentNode.equalsIgnoreCase(rootPrompt)){
      if(!command.startsWith("CREATE")){
        return new SolReturn(rootPrompt, "Only valid commands are CREATE");
      } else {
        currentNode = planPrompt;
        String name = command.substring(command.indexOf(' ')+1);
        thePlan.setName(name);
        return new SolReturn(planPrompt,"");
      }
    }
    if (currentNode.equalsIgnoreCase(planPrompt)){
      if ("CREATE".equalsIgnoreCase(parts[0])){

        if (parts[1].equalsIgnoreCase("feed")){
          //CREATE FEED myFeed using teknek.kafka.feed
          String name = parts[2];
          String className = parts[4];
          FeedDesc feed = new FeedDesc();
          feed.setProperties(new TreeMap());
          feed.setFeedClass(className);
          thePlan.setFeedDesc(feed);
          currentNode = feedPrompt;
          return new SolReturn(feedPrompt,"");
        }    
      }
    }
    if (currentNode.equalsIgnoreCase(feedPrompt)){
      if ("EXIT".equalsIgnoreCase(parts[0])){
        currentNode = planPrompt;
        return new SolReturn(planPrompt,"");
      }
      if (parts[0].equalsIgnoreCase("SET")){
        //SET PROPERTY topic AS 'firehoze';
        String name = parts[2];
        String val = parts[4];
        try { 
          if (val.startsWith("'")) {
            thePlan.getFeedDesc().getProperties().put(name, val.replace("'", ""));
            return new SolReturn(feedPrompt, "");
          } else {
            thePlan.getFeedDesc().getProperties().put(name, Double.parseDouble(val.replace("'", "")));
            return new SolReturn(feedPrompt, "");
          }
        } catch (Exception ex){
          return new SolReturn(feedPrompt, "problem settting property "+ex.getMessage());
        }
      }
    }
    //throw new RuntimeException("I am lost! currentNode "+currentNode +" command"+command);
    return new SolReturn(currentNode,"I am lost! currentNode "+currentNode +" command "+command);
  }
  
  public static void main(String[] args) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    Sol s = new Sol();
    String line = null;
    while ((line = br.readLine()) != null) {
      SolReturn ret = s.send(line);
      if (ret.getMessage().length()>0){
        System.out.println(ret.getMessage());
      }
      System.out.print(ret.getPrompt());
    }
  }
  
}


class SolReturn {
  private String prompt;
  private String message;
  
  public SolReturn(){
    
  }
  public SolReturn(String pr, String mess){
    prompt=pr;
    message = mess;
  }
  
  public String getPrompt() {
    return prompt;
  }
  public void setPrompt(String prompt) {
    this.prompt = prompt;
  }
  public String getMessage() {
    return message;
  }
  public void setMessage(String message) {
    this.message = message;
  }
  
}
