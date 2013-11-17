package io.teknek.daemon;

public class WorkerStatus {
  private String workerUuid;

  private String feedPartitionId;

  public WorkerStatus(String workerUuid, String feedPartitionId) {
    this.workerUuid = workerUuid;
    this.feedPartitionId = feedPartitionId;
  }

  public String getWorkerUuid() {
    return workerUuid;
  }

  public void setWorkerUuid(String workerUuid) {
    this.workerUuid = workerUuid;
  }

  public String getFeedPartitionId() {
    return feedPartitionId;
  }

  public void setFeedPartitionId(String feedPartitionId) {
    this.feedPartitionId = feedPartitionId;
  }

}
