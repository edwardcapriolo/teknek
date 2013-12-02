package io.teknek.sol;

public class SolReturn {
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

