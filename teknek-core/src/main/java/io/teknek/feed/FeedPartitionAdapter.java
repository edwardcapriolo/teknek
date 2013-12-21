package io.teknek.feed;

import io.teknek.model.ITuple;

public class FeedPartitionAdapter extends FeedPartition {

  public FeedPartitionAdapter(Feed feed, String partitionId) {
    super(feed, partitionId);
  }

  @Override
  public void initialize() {
  }

  @Override
  public boolean next(ITuple tupleRef) {
    return false;
  }

  @Override
  public void close() {
  }

  @Override
  public boolean supportsOffsetManagement() {
    return false;
  }

  @Override
  public String getOffset() {
    return null;
  }

  @Override
  public void setOffset(String offset) {
  }

}
