package io.teknek.kafka;

import kafka.utils.Time;

/** used in integration tests to provide true time source **/
public class TimeImpl implements Time { 

  public TimeImpl(){
    
  }

  @Override
  public long milliseconds() {
    return System.currentTimeMillis();
  }

  @Override
  public long nanoseconds() {
    return System.nanoTime();
  }

  @Override
  public void sleep(long arg0) {
    try {
      Thread.sleep(arg0);
    } catch (Exception ex){}
  }
  
};
