/*
Copyright 2013 Edward Capriolo, Matt Landolf, Lodwin Cueto

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
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
