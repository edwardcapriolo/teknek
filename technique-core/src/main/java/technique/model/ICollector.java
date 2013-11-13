package technique.model;

/**
 * A component that accepts tuples potentially storing or forwarding
 * either way the client is un-aware
 *
 */ 
public abstract class ICollector {
  /**
   * Note: we may remove the source later
   * @param out
   */
  public abstract void emit(Tuple out);
}

