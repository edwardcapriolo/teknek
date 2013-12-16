package io.teknek.sol;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
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
    String [] parts = command.split("\\s+");
    if (command.equalsIgnoreCase("SHOW")){
      return new SolReturn(currentNode, new String(WorkerDao.serializePlan(thePlan)));
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
      if("create".equalsIgnoreCase(parts[0])){
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
        ///teknek/saved/group-name-type
        //load io.teknek MyOperator operator
        String group = parts[1];
        String name = parts[2];
        String type = parts[3];
        
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
        String opName = parts[1];
        String child = parts[4];
        if (!operators.containsKey(opName)){
          return new SolReturn(currentNode, opName + " is not the name of an operator");
        }
        if (!operators.containsKey(child)){
          return new SolReturn(currentNode, child + " is not the name of an operator");
        }
        this.operators.get(opName).getChildren().add(operators.get(child));
        return new SolReturn(currentNode, "" );
      }
      
    }
    
    if (currentNode.equalsIgnoreCase(operatorPrompt)){
 
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
    }
    
    if (currentNode.equalsIgnoreCase(inlinePrompt)){
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
    
    if (currentNode.equalsIgnoreCase(operatorPrompt)){
      if (parts[0].equalsIgnoreCase("exit")){
        currentNode = planPrompt;
        return new SolReturn(planPrompt,"");
      }
    }
    if (currentNode.equalsIgnoreCase(feedPrompt)){
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

  public ZooKeeper getZookeeper() {
    return zookeeper;
  }

  public void setZookeeper(ZooKeeper zookeeper) {
    this.zookeeper = zookeeper;
  }
  
  
}
