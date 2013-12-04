package io.teknek.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.teknek.feed.Feed;
import io.teknek.feed.FeedPartition;
import io.teknek.model.ITuple;

public class HdfsFeed extends Feed {

  public static final String OUTPUT_TUPLE_NAME = "line";
  public static final String applyToConf = "apply.to.conf"; 
  public static final String FEED_DIR= "hdfsfeed.base.dir";
  public static final String NUMBER_PARTITIONS = "hdfsfeed.partitions";
  private String baseDir;
  private Integer partitions;

  public HdfsFeed(Map<String, Object> properties) {
    super(properties);
    baseDir = properties.get(FEED_DIR).toString();
    partitions = Integer.valueOf(properties.get(NUMBER_PARTITIONS).toString());
  }

  @Override
  public List<FeedPartition> getFeedPartitions() {
    List<FeedPartition> feeds = new ArrayList<FeedPartition>();
    for (int i = 0; i < partitions; i++) {
      feeds.add (new HdfsFeedPartition(this, i+""));
    }
    return feeds;
  }

  @Override
  public Map<String, String> getSuggestedBindParams() {
    return new HashMap<String,String>();
  }

  public String getBaseDir() {
    return baseDir;
  }

  public void setBaseDir(String baseDir) {
    this.baseDir = baseDir;
  }

  public Integer getPartitions() {
    return partitions;
  }

  public void setPartitions(Integer partitions) {
    this.partitions = partitions;
  }
  
}

class HdfsFeedPartition extends FeedPartition {

  private Path workingOn;
  private BufferedReader br = null;
  private Configuration configuration;
  
  public HdfsFeedPartition(Feed feed, String partitionId) {
    super(feed, partitionId);
    configuration = new Configuration();
    for ( Map.Entry<String, Object> entry: feed.getProperties().entrySet() ) {
      if (entry.getKey().startsWith(HdfsFeed.applyToConf)){
        String conf = entry.getKey().substring((HdfsFeed.applyToConf + ".").length());
        configuration.set(conf, entry.getValue().toString());
      }
      
    }
  }

  @Override
  public void close() {
  }

  @Override
  public void initialize() {

  }

  /**
   * Builds a list of files inside a directory who's name hashes to this partition id
   * pick first in list that you have not seen
   * @param arg0
   * @return
   */
  @Override
  public boolean next(ITuple arg0) {
    while(true){
      if (br == null){
        workingOn = getNextFile();
        FileSystem fs = null;
        try {
          fs = FileSystem.get(configuration);
          br = new BufferedReader(new InputStreamReader(fs.open(workingOn)));
        } catch (IOException e) {
          e.printStackTrace();
        }    
      }
      try {
        String line = null;
        while ((line = br.readLine()) != null) {
          arg0.setField(HdfsFeed.OUTPUT_TUPLE_NAME, line);
          return true;
        }
        br.close();
        br = null;
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public Path getNextFile(){
    HdfsFeed h = (HdfsFeed) feed;
    Path p = new Path(h.getBaseDir());
    Path toProcess = null;
    try {
      FileSystem fs = FileSystem.get(configuration);
      FileStatus [] status = fs.listStatus(p);

      while (toProcess == null){
        int index = 0;
        while (index < status.length){
          if (status[index].getPath().getName().hashCode() % h.getPartitions() == Integer.parseInt(this.getPartitionId())){
            if (workingOn == null) {
              toProcess = status[index].getPath();
              return toProcess;
            } else if (workingOn.getName().compareTo(status[index].getPath().getName()) < 0){
              toProcess = status[index].getPath();
              return toProcess;
            }
          }
          index++;
        }
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return toProcess;
  }
  
  @Override
  public boolean supportsOffsetManagement() {
    return true;
  }
  
  @Override
  public String getOffset() {
    return workingOn.getName();
  }
  
  @Override
  public void setOffset(String arg0) {
    workingOn = new Path(arg0);
  }

  public Configuration getConfiguration() {
    return configuration;
  }
  
}