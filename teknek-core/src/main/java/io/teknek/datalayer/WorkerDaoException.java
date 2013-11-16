package io.teknek.datalayer;

public class WorkerDaoException extends Exception {

  private static final long serialVersionUID = 1L;

  public WorkerDaoException() {
    super();
  }

  public WorkerDaoException(String message, Throwable cause, boolean enableSuppression,
          boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public WorkerDaoException(String message, Throwable cause) {
    super(message, cause);
  }

  public WorkerDaoException(String message) {
    super(message);
  }

  public WorkerDaoException(Throwable cause) {
    super(cause);
  }
  
  

}
