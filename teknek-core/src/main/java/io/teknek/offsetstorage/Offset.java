package io.teknek.offsetstorage;

public abstract class Offset {
  public Offset(byte [] bytes){
    
  }
  public abstract byte [] serialize();
  
}
