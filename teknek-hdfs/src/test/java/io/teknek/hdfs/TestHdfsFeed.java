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

  private void writebFile() throws IOException {
    File a = folder.newFile("b.txt");
    BufferedWriter out = new BufferedWriter(new FileWriter(a));
    out.write("a\n");
    out.write("b\n");
    out.close();
  }

  private void writeCFile() throws IOException {
    File b = folder.newFile("c.txt");
    BufferedWriter out = new BufferedWriter(new FileWriter(b));
    out.write("d\n");
    out.write("e\n");
    out.close();
  }

  @Test
  public void tryWithTwoFilesOnePartition() throws IOException {
    writebFile();
    writeCFile();
    HdfsFeed f = new HdfsFeed(MapBuilder.makeMap(HdfsFeed.NUMBER_PARTITIONS, 1, HdfsFeed.FEED_DIR,
            folder.getRoot().getPath()));
    List<FeedPartition> parts = f.getFeedPartitions();
    ITuple it = new Tuple();
    Assert.assertEquals(true, parts.get(0).next(it));
    Assert.assertEquals("a", it.getField(HdfsFeed.OUTPUT_TUPLE_NAME));
    parts.get(0).next(it);
    Assert.assertEquals("b", it.getField(HdfsFeed.OUTPUT_TUPLE_NAME));
    parts.get(0).next(it);
    Assert.assertEquals("d", it.getField(HdfsFeed.OUTPUT_TUPLE_NAME));
    parts.get(0).next(it);
    Assert.assertEquals("e", it.getField(HdfsFeed.OUTPUT_TUPLE_NAME));
  }

  @Test
  public void tryWithTwoFilesTwoPartition() throws IOException {
    writebFile();
    writeCFile();
    HdfsFeed f = new HdfsFeed(MapBuilder.makeMap(HdfsFeed.NUMBER_PARTITIONS, 2, HdfsFeed.FEED_DIR,
            folder.getRoot().getPath()));
    List<FeedPartition> parts = f.getFeedPartitions();
    ITuple it = new Tuple();
    Assert.assertEquals(true, parts.get(0).next(it));
    Assert.assertEquals("a", it.getField(HdfsFeed.OUTPUT_TUPLE_NAME));
    Assert.assertEquals(true, parts.get(0).next(it));
    Assert.assertEquals("b", it.getField(HdfsFeed.OUTPUT_TUPLE_NAME));
    Assert.assertEquals(true, parts.get(1).next(it));
    Assert.assertEquals("d", it.getField(HdfsFeed.OUTPUT_TUPLE_NAME));
    Assert.assertEquals(true, parts.get(1).next(it));
    Assert.assertEquals("e", it.getField(HdfsFeed.OUTPUT_TUPLE_NAME));
  }

  @Test
  public void tryWithOneFilesOnePartition() throws IOException {
    writebFile();
    HdfsFeed f = new HdfsFeed(MapBuilder.makeMap(HdfsFeed.NUMBER_PARTITIONS, 1, HdfsFeed.FEED_DIR,
            folder.getRoot().getPath()));
    List<FeedPartition> parts = f.getFeedPartitions();
    ITuple it = new Tuple();
    Assert.assertEquals(true, parts.get(0).next(it));
    Assert.assertEquals("a", it.getField(HdfsFeed.OUTPUT_TUPLE_NAME));
    Assert.assertEquals(true, parts.get(0).next(it));
    Assert.assertEquals("b", it.getField(HdfsFeed.OUTPUT_TUPLE_NAME));
  }

  @Test
  public void testApplyToConf() {
    String varName = "setthis";
    String varValue = "that";
    HdfsFeed f = new HdfsFeed(MapBuilder.makeMap(HdfsFeed.NUMBER_PARTITIONS, 2, HdfsFeed.FEED_DIR,
            folder.getRoot().getPath(), HdfsFeed.applyToConf + "." + varName, varValue));
    Assert.assertEquals(varValue, ((HdfsFeedPartition) f.getFeedPartitions().get(0))
            .getConfiguration().get(varName));
  }

}
