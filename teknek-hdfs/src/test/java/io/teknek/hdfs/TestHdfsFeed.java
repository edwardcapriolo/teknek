package io.teknek.hdfs;

import io.teknek.feed.FeedPartition;
import io.teknek.model.ITuple;
import io.teknek.model.Tuple;
import io.teknek.util.MapBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class TestHdfsFeed {


  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  
  @Test
  public void tryWithOneFile() throws IOException{
    
    {
      File a = folder.newFile("b.txt");
      BufferedWriter out = new BufferedWriter(new FileWriter(a));
      out.write("a\n");
      out.write("b\n");
      out.close();
    }
    
    {
      File b = folder.newFile("c.txt");
      BufferedWriter out = new BufferedWriter(new FileWriter(b));
      out.write("d\n");
      out.write("e\n");
      out.close();
    }
    
    HdfsFeed f = new HdfsFeed(MapBuilder.makeMap(HdfsFeed.NUMBER_PARTITIONS,1,HdfsFeed.FEED_DIR, folder.getRoot().getPath()));
    

    List<FeedPartition> parts = f.getFeedPartitions();
    ITuple it = new Tuple();
    Assert.assertEquals(true, parts.get(0).next(it));
    Assert.assertEquals("a", it.getField("line"));
    System.out.println(it);
    
    Assert.assertEquals(true, parts.get(0).next(it));
    Assert.assertEquals("b", it.getField("line"));
    System.out.println(it);
    
    Assert.assertEquals(true, parts.get(0).next(it));
    Assert.assertEquals("d", it.getField("line"));
    System.out.println(it);
    
    Assert.assertEquals(true, parts.get(0).next(it));
    Assert.assertEquals("e", it.getField("line"));
    System.out.println(it);
    
  }
  
}
