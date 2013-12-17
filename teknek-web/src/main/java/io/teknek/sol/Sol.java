package io.teknek.sol;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.zookeeper.ZooKeeper;
import org.codehaus.jackson.map.ObjectMapper;

import io.teknek.datalayer.WorkerDao;
import io.teknek.datalayer.WorkerDaoException;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;

public class Sol {

  public static final String rootPrompt = "teknek> ";
  public static final String planPrompt = "plan> ";
  public static final String feedPrompt = "feed> ";
  public static final String operatorPrompt = "operator> ";
  public static final String inlinePrompt = "inline> "; 
  
  String currentNode;
  private Plan thePlan;
  private Map<String,OperatorDesc> operators;
  private ZooKeeper zookeeper;
  private OperatorDesc currentOperator;
  private StringBuilder inline;
  
  public Sol(){
    thePlan = new Plan();
    currentNode = rootPrompt;
    operators = new HashMap<String,OperatorDesc>();
  }
  
  
  public SolReturn send(String command){
    try {
      String [] parts = command.split("\\s+");
      /* We want to keep this first because the data is free form 
       * and could be misinterpreted as something else
       */
      if ( currentNode.equalsIgnoreCase(inlinePrompt)){
        processInline(parts, command);
      }
      /* next comes global commands that can be called at any level */
      if ("SHOW".equalsIgnoreCase(parts[0])){
        //return new SolReturn(currentNode, new String(WorkerDao.serializePlan(thePlan)));
        return processShow(parts);
      }
      if ("save".equalsIgnoreCase(parts[0])){
        try {
          WorkerDao.createOrUpdatePlan(thePlan, zookeeper);
        } catch (WorkerDaoException e) {
          return new SolReturn(currentNode, "problem persisting "+e.getMessage());
        }
        return new SolReturn(currentNode, "");
      }
      if ("open".equalsIgnoreCase(parts[0])){
        String plan = parts[1];
        try {
          thePlan = WorkerDao.findPlanByName(zookeeper, plan);
        } catch (WorkerDaoException e) {
          return new SolReturn(currentNode, "problem reading "+e.getMessage());
        }
        return new SolReturn(currentNode, "");
      }
      if (currentNode.equalsIgnoreCase(rootPrompt)){
        return processRoot(parts, command);
      }
      if (currentNode.equalsIgnoreCase(planPrompt)){
        return processPlan(parts);
      }
      if (currentNode.equalsIgnoreCase(operatorPrompt)){
        return processOperator(parts);
      }
      if (currentNode.equalsIgnoreCase(feedPrompt)){
        return processFeed(parts);
      }
    } catch (RuntimeException ex) {
      ex.printStackTrace();
      return new SolReturn(currentNode,"Parsing command failed "+ex.getMessage());
    }
    return new SolReturn(currentNode,"I am lost! currentNode "+currentNode +" command "+command);
  }
  
  private SolReturn processShow(String [] parts){
    if (parts.length ==3 && parts[1].equalsIgnoreCase("CURRENT") && parts[2].equalsIgnoreCase("PLAN")){
      return new SolReturn(currentNode, new String(WorkerDao.serializePlan(thePlan)));
    }
    if (parts.length == 2 && parts[1].equalsIgnoreCase("PLANS")){
      StringBuilder sb = new StringBuilder();
      List<String> plans = null; 
      try {
        plans = WorkerDao.finalAllPlanNames(zookeeper);
      } catch (WorkerDaoException e) {
        return new SolReturn(currentNode, e.getMessage());
      }
      for (String plan : plans){
        sb.append(plan+"\n");
      }
      return new SolReturn(currentNode, sb.toString());
    }
    return new SolReturn(planPrompt, "Command not found");
  }
  
  private SolReturn processRoot(String [] parts, String command){
    if("create".equalsIgnoreCase(parts[0])){
      currentNode = planPrompt;
      String name = command.substring(command.indexOf(' ')+1);
      thePlan.setName(name);
      return new SolReturn(planPrompt,"");
    }
    return new SolReturn(planPrompt, "Command not found");
  }
  
  
  /** These methods are only available from the plan prompt */
  private SolReturn processPlan(String [] parts){
    if ("CREATE".equalsIgnoreCase(parts[0])){
      if (parts[1].equalsIgnoreCase("feed")){
        //CREATE FEED myFeed using teknek.kafka.feed
        String name = parts[2];
        String className = parts[4];
        FeedDesc feed = new FeedDesc();
        feed.setProperties(new TreeMap());
        feed.setTheClass(className);
        thePlan.setFeedDesc(feed);
        currentNode = feedPrompt;
        return new SolReturn(feedPrompt,"");
      }
      if (parts[1].equalsIgnoreCase("operator")){
        //CREATE OPERATOR plus2 AS teknek.samples.Plus2;
        String name = parts[2];
        String className = parts[4];
        OperatorDesc desc = new OperatorDesc();
        desc.setTheClass(className);
        operators.put(name, desc);
        currentNode = operatorPrompt;
        currentOperator = desc; //when we exit reset this to null
        return new SolReturn(operatorPrompt, "");
      }
    }
    
    if ("LOAD".equalsIgnoreCase(parts[0])){
      //load io.teknek MyOperator operator as plus2
      String group = parts[1];
      String name = parts[2];
      //String type = parts[3];
      String register = parts[5];
      OperatorDesc desc = null;
      try {
        desc = WorkerDao.loadSavedOperatorDesc(zookeeper, group, name);
      } catch (WorkerDaoException e) {
        return new SolReturn(currentNode, e.getMessage());
      }
      operators.put(register, desc);
      currentNode = operatorPrompt;
      currentOperator = desc; //when we exit reset this to null
      return new SolReturn(operatorPrompt, "");
    }
    
    if ("SET".equalsIgnoreCase(parts[0])){
      //myPlan> SET ROOT plus2;
      String opName = parts[2];
      if (!operators.containsKey(opName)){
        return new SolReturn(currentNode, opName + " is not the name of an operator");
      }
      thePlan.setRootOperator(operators.get(opName));
      return new SolReturn(currentNode, "");
      
    }
    if ("FOR".equalsIgnoreCase(parts[0])){
      //myPlan> FOR plus2 ADD CHILD times5;
      //myPlan> FOR plus2 REMOVE CHILD times5;
      String opName = parts[1];
      String op = parts[2];
      String child = parts[4];
      if (!operators.containsKey(opName)){
        return new SolReturn(currentNode, opName + " is not the name of an operator");
      }
      if (!operators.containsKey(child)){
        return new SolReturn(currentNode, child + " is not the name of an operator");
      }
      if (op.equalsIgnoreCase("ADD")){
        operators.get(opName).getChildren().add(operators.get(child));
      } else if (op.equalsIgnoreCase("REMOVE")){
        operators.get(opName).getChildren().remove(operators.get(child));
      } else {
        return new SolReturn(currentNode, "op must be ADD or REMOVE. You specified  "+op );
      }
      return new SolReturn(currentNode, "" );
    }
    
    return new SolReturn(currentNode, "No match found" );
  }
  
  /**
   * Processing for the operator prompt
   * @param parts
   * @return
   */
  private SolReturn processOperator(String [] parts){
    if (parts[0].equalsIgnoreCase("set")){
      //set operatorspec as groovyclosure
      String type = parts[3];
      if (type.equalsIgnoreCase("groovyclosure")){
        currentOperator.setSpec(parts[3]);
        return new SolReturn(operatorPrompt, "");
      } else if (type.equalsIgnoreCase("groovyclass")){
        currentOperator.setSpec(parts[3]);
        return new SolReturn(operatorPrompt, "");
      }
    }
    if (parts[0].equalsIgnoreCase("inline")){
      currentNode = inlinePrompt;
      inline = new StringBuilder();
      return new SolReturn(inlinePrompt, "Define script below. End script with -----");
    }
    if (parts[0].equalsIgnoreCase("save_operator")){
      //save bundlename name
      String bundle = parts[1];
      String name = parts[2];
      try {
        WorkerDao.saveOperatorDesc(zookeeper, currentOperator, bundle, name);
      } catch (WorkerDaoException e) {
        return new SolReturn(operatorPrompt, e.getMessage());
      }
      return new SolReturn(operatorPrompt, "");
    }
    if (parts[0].equalsIgnoreCase("exit")){
      currentNode = planPrompt;
      return new SolReturn(planPrompt,"");
    }
    return new SolReturn(operatorPrompt, "Command not found");
  }
  
  private SolReturn processInline(String [] parts, String command){
    if (parts[0].equals("-----")){
      //attempt to compile buffer //maybe
      currentOperator.setScript(this.inline.toString());
      this.inline = null;
      currentNode = operatorPrompt;
      return new SolReturn(operatorPrompt, "");
    } else {
      inline.append(command+"\n");
      return new SolReturn(inlinePrompt, "");
    }
  }
  
  private SolReturn processFeed(String [] parts){
    if ("EXIT".equalsIgnoreCase(parts[0])){
      currentNode = planPrompt;
      return new SolReturn(planPrompt,"");
    }
    //TODO unset here
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
    return new SolReturn(feedPrompt, "command not found");
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

  public ZooKeeper getZookeeper() {
    return zookeeper;
  }

  public void setZookeeper(ZooKeeper zookeeper) {
    this.zookeeper = zookeeper;
  }
  
  
}
