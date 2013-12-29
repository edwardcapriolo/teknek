package io.teknek.cassandra;

import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;

public class TestTimedGroupByEngine extends EmbeddedCassandraServer{

  @Test
  public void aTest() throws ConnectionException{
    
    TimedGroupByEngine tg = new TimedGroupByEngine(KEYSPACE, 9157, "localhost:9157");
    {
      TimedGroupEntry e = new TimedGroupEntry();
      GregorianCalendar gc = new GregorianCalendar();
      gc.set(Calendar.YEAR, 1981);
      gc.set(Calendar.MONTH, 2);
      gc.set(Calendar.DAY_OF_MONTH, 12);
      gc.set(Calendar.HOUR, 4);
      gc.set(Calendar.MINUTE, 1);
      
      e.setEventTimeInMillis(gc.getTimeInMillis());
      e.getEventProperties().put("host", "web1");
      e.getEventProperties().put("metric", "hits");
      e.getGroups().add(Arrays.asList("host"));
      e.getGroups().add(Arrays.asList("host","metric"));
      e.getGroups().add(Arrays.asList("metric","host"));
      e.getGroups().add(Arrays.asList("metric"));
      e.setIncrementValue(10);
      tg.write(e);
    }
    {
      TimedGroupEntry e = new TimedGroupEntry();
      GregorianCalendar gc = new GregorianCalendar();
      gc.set(Calendar.YEAR, 1981);
      gc.set(Calendar.MONTH, 2);
      gc.set(Calendar.DAY_OF_MONTH, 12);
      gc.set(Calendar.HOUR, 4);
      gc.set(Calendar.MINUTE, 1);
      
      e.setEventTimeInMillis(gc.getTimeInMillis());
      e.getEventProperties().put("host", "web1");
      e.getEventProperties().put("metric", "misses");
      e.getGroups().add(Arrays.asList("host"));
      e.getGroups().add(Arrays.asList("host","metric"));
      e.getGroups().add(Arrays.asList("metric","host"));
      e.getGroups().add(Arrays.asList("metric"));
      e.setIncrementValue(23);
      tg.write(e);
    }
    
    
    
    {
      TimedGroupEntry e = new TimedGroupEntry();
      GregorianCalendar gc = new GregorianCalendar();
      gc.set(Calendar.YEAR, 1981);
      gc.set(Calendar.MONTH, 2);
      gc.set(Calendar.DAY_OF_MONTH, 12);
      gc.set(Calendar.HOUR, 3);
      gc.set(Calendar.MINUTE, 3);
      
      e.setEventTimeInMillis(gc.getTimeInMillis());
      e.getEventProperties().put("host", "web1");
      e.getEventProperties().put("metric", "hits");
      e.getGroups().add(Arrays.asList("host"));
      e.getGroups().add(Arrays.asList("host","metric"));
      e.getGroups().add(Arrays.asList("metric","host"));
      e.getGroups().add(Arrays.asList("metric"));
      e.setIncrementValue(11);
      tg.write(e);
    }
    
    {
      TimedGroupEntry e = new TimedGroupEntry();
      GregorianCalendar gc = new GregorianCalendar();
      gc.set(Calendar.YEAR, 1981);
      gc.set(Calendar.MONTH, 2);
      gc.set(Calendar.DAY_OF_MONTH, 12);
      gc.set(Calendar.HOUR, 4);
      gc.set(Calendar.MINUTE, 3);
      
      e.setEventTimeInMillis(gc.getTimeInMillis());
      e.getEventProperties().put("host", "web1");
      e.getEventProperties().put("metric", "hits");
      e.getGroups().add(Arrays.asList("host"));
      e.getGroups().add(Arrays.asList("host","metric"));
      e.getGroups().add(Arrays.asList("metric","host"));
      e.getGroups().add(Arrays.asList("metric"));
      e.setIncrementValue(12);
      tg.write(e);
    }
    
    
    GregorianCalendar gc = new GregorianCalendar();
    gc.set(Calendar.YEAR, 1981);
    gc.set(Calendar.MONTH, 2);
    gc.set(Calendar.DAY_OF_MONTH, 12);
    gc.set(Calendar.HOUR, 4);
    gc.set(Calendar.MINUTE, 1);
    
    String rowKey = TimedGroupByEngine.newByHourDateFormat().format(gc.getTime());
    
    ColumnList<String> result = keyspace.prepareQuery(TimedGroupByEngine.byHour)
            .getKey(rowKey)
            .execute().getResult();
   
    Assert.assertEquals(7, result.size());
    Assert.assertTrue(result.getColumnNames().contains("host#web1"));
    Assert.assertEquals(result.getColumnByName("host#web1").getLongValue(), 45);
    for (String name : result.getColumnNames()){
      System.out.println(name + " " + result.getColumnByName(name).getLongValue());
    }

    List<Map<String,String>> res = tg.queryByDay(gc.getTime(), "metric#", "metric#~");
    Assert.assertEquals(22+"", res.get(0).get("hits"));
    Assert.assertEquals(23+"", res.get(0).get("misses"));
    
    
    List<Map<String,String>> res2 = tg.queryByDayWithFilter(gc.getTime(), "metric#", "metric#~", "hits");
    Assert.assertEquals(22+"", res2.get(0).get("hits"));
    Assert.assertFalse(res2.contains("misses"));
    
    
  }
}
