package io.teknek.test;

public abstract class WaitForCondition {

  private int waited = 0;
  
  public WaitForCondition(){
    waited = 0;
  }
  
  /** while false keep waiting*/
  public abstract boolean condition();
  
  public void waitFor(int millis) {
    while (!condition() && waited++ < millis) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
      }
    }
  }

  public int getWaited() {
    return waited;
  }

  public void setWaited(int waited) {
    this.waited = waited;
  }
  
}
