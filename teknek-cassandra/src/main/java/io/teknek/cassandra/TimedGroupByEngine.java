package io.teknek.cassandra;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class TimedGroupByEngine {
  private Keyspace keyspace;

  public static final String byMinuteString = "yyyy-MM-dd-HH-mm";
  public static final String byHourString = "yyyy-MM-dd-HH";
  public static final String byDayString = "yyyy-MM-dd";
  
  public static final ColumnFamily<String, String> byMinute = new ColumnFamily<String, String>(
          "StatsByMinute", StringSerializer.get(), StringSerializer.get(), LongSerializer.get());

  public static final ColumnFamily<String, String> byHour = new ColumnFamily<String, String>(
          "StatsByHour", StringSerializer.get(), StringSerializer.get(), LongSerializer.get());

  public static final ColumnFamily<String, String> byDay = new ColumnFamily<String, String>(
          "StatsByDay", StringSerializer.get(), StringSerializer.get(), LongSerializer.get());

  public TimedGroupByEngine(String ks, int port, String hostlist){
    AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
    .forCluster("ClusterName")
    .forKeyspace(ks)
    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
    )
    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
        .setPort(port)
        .setMaxConnsPerHost(1)
        .setSeeds(hostlist)
    )
    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
    .buildKeyspace(ThriftFamilyFactory.getInstance() );
    context.start();
    keyspace = context.getEntity();
  }

  public static DateFormat newByMinuteDateFormat(){
    return new SimpleDateFormat(byMinuteString);
  }
  
  public static DateFormat newByHourDateFormat(){
    return new SimpleDateFormat(byHourString);
  }
  
  public static DateFormat newByDayDateFormat(){
    return new SimpleDateFormat(byHourString);
  }
  
  public void write(TimedGroupEntry e){
    MutationBatch m = keyspace.prepareMutationBatch();
    DateFormat byMinuteDateFormat = newByMinuteDateFormat();
    DateFormat byHourDateFormat = newByHourDateFormat();
    DateFormat byDayDateFormat = newByDayDateFormat();  
   
    for (List<String> group : e.getGroups()) {
      StringBuilder column = new StringBuilder();
      StringBuilder value = new StringBuilder();
      for (int i = 0; i < group.size(); i++) {
        column.append(group.get(i));
        value.append(e.getEventProperties().get(group.get(i)));
        if (i < group.size() - 1) {
          column.append("+");
          value.append("+");
        }
      }
      StringBuilder end = new StringBuilder().append(column).append("#").append(value);
      m.withRow(byMinute, byMinuteDateFormat.format(new Date(e.getEventTimeInMillis()))).incrementCounterColumn(end.toString(), e.getIncrementValue());
      m.withRow(byHour, byHourDateFormat.format(new Date(e.getEventTimeInMillis()))).incrementCounterColumn(end.toString(), e.getIncrementValue());
      m.withRow(byDay, byDayDateFormat.format(new Date(e.getEventTimeInMillis()))).incrementCounterColumn(end.toString(), e.getIncrementValue());
    }
    try {
      OperationResult<Void> result = m.execute();
    } catch (ConnectionException ex) {
      ex.printStackTrace();
    }
  }
  
  public List<Map<String,String>> queryByDayWithFilter(Date start, String sliceStart, String sliceEnd, String filter){
    List<Map<String,String>> par = queryByDay(start, sliceStart, sliceEnd);
    String [] parts = filter.split(",");
    for (String part: parts){
      par.remove(part);
    }
    return par;
  }
  
  
  public List<Map<String,String>> queryByDay(Date start, String sliceStart, String sliceEnd){
    
    String rowKey = TimedGroupByEngine.newByHourDateFormat().format(start);
    ColumnList<String> result = null;
    List<Map<String,String>> results = new ArrayList<>();
    try {
      result = keyspace.prepareQuery(TimedGroupByEngine.byHour)
      .getKey(rowKey).withColumnRange(sliceStart, sliceEnd, false, 10000)
      .execute().getResult();
    } catch (ConnectionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    Map<String, String> oneRow = new HashMap<String, String>();
    oneRow.put("timeInMillis", start.getTime() + "");
    for (String name : result.getColumnNames()){
      System.out.println(name + " " + result.getColumnByName(name).getLongValue());
      oneRow.put(name.split("#")[1], result.getColumnByName(name).getLongValue()+"");
    }
    results.add(oneRow);
    return results;
    /*
    host+metric#web1+hits 22
    host#web1 22
    metric#hits 22
    metric+host#hits+web1 22 */
    
    /*
    ColumnList<String> result = keyspace.prepareQuery(TimedGroupByEngine.byHour)
            .getKey(rowKey)
            .execute().getResult();
    */
    //stats[yyyy--mm--dd][mm+eventName#eventValue]=7
    //stats[yyyy--mm--dd][mm+age+weight#21+180]=7
    
    //StatsByMinute[yyyy-mm-dd-hh-mm][age+weight#20]
    //StatsByHour[yyyy-mm-dd-hh][age+weight#20]
    //StatsByDay[yyyy-mm-dd][age+weight#20]
            
    
    
  }
}