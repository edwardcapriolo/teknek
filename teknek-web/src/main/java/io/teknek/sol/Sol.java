package io.teknek.sol;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import io.teknek.datalayer.WorkerDao;
import io.teknek.plan.Plan;

public class Sol {

  String currentNode;
  public static final String rootPrompt = "teknek>";
  public static final String planPrompt = "plan>";
  
  private Plan thePlan;
  public Sol(){
    thePlan = new Plan();
    currentNode = rootPrompt;
  }
  
  public SolReturn send(String command){
    if (command.equalsIgnoreCase("SHOW")){
      return new SolReturn(currentNode, new String( WorkerDao.serializePlan(thePlan)));
    }
    if (currentNode.equalsIgnoreCase(rootPrompt)){
      if(!command.startsWith("CREATE")){
        return new SolReturn(rootPrompt, "Only valid commands are CREATE");
      } else {
        currentNode = "plan";
        String name = command.substring(command.indexOf(' ')+1);
        thePlan.setName(name);
        return new SolReturn(planPrompt,"");
      }
    }
    throw new RuntimeException("I am lost! currentNode "+currentNode +" command"+command);
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
