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
package io.teknek.plan;

import io.teknek.datalayer.WorkerDao;
import io.teknek.driver.Minus1Operator;
import io.teknek.driver.TestDriver;
import io.teknek.driver.Times2Operator;
import io.teknek.driver.exception.ExceptionOperator;
import io.teknek.feed.FixedFeed;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;
import io.teknek.util.MapBuilder;
import io.teknek.zookeeper.ZookeeperOffsetStorage;

import java.io.IOException;

import junit.framework.Assert;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

public class TestPlan {

  public static Plan getPlan(){
    Plan p = new Plan()
    .withFeedDesc(
            new FeedDesc().withFeedClass(FixedFeed.class.getName()).withProperties(
                    MapBuilder.makeMap(FixedFeed.NUMBER_OF_PARTITIONS, 2, FixedFeed.NUMBER_OF_ROWS,
                            10))).withRootOperator(
            new OperatorDesc(new Minus1Operator()).withNextOperator(new OperatorDesc(
                    new Times2Operator())));
    p.setName("myplan");
    return p;
  }
  
  /**
   * This second plan was added because other tests use the first plan, and may not bring up zk 
   * in there tests
   * @return a Plan
   */
  public static Plan getPlanWithZookeeperStorage(){
    Plan p = new Plan()
    .withOffsetStorageDesc(new OffsetStorageDesc().withOperatorClass(ZookeeperOffsetStorage.class.getName())
            .withParameters(MapBuilder.makeMap(ZookeeperOffsetStorage.ZK_CONNECT, "localhost:2181")) )
    .withFeedDesc(
            new FeedDesc().withFeedClass(FixedFeed.class.getName()).withProperties(
                    MapBuilder.makeMap(FixedFeed.NUMBER_OF_PARTITIONS, 2, FixedFeed.NUMBER_OF_ROWS,
                            10))).withRootOperator(
            new OperatorDesc(new Minus1Operator()).withNextOperator(new OperatorDesc(
                    new Times2Operator())));
    p.setName("myplan");
    return p;
  }
  
  public static Plan getPlanWithException(){
    Plan plan = new Plan()
    .withFeedDesc( new FeedDesc()
      .withFeedClass(FixedFeed.class.getName())
        .withProperties( MapBuilder.makeMap(FixedFeed.NUMBER_OF_PARTITIONS, 2, FixedFeed.NUMBER_OF_ROWS,10)))
     .withRootOperator( new OperatorDesc(new Minus1Operator())
       .withNextOperator(new OperatorDesc(new Times2Operator())
         .withNextOperator(new OperatorDesc(new ExceptionOperator()))));
    plan.setName("failedplan");
    return plan;
  }
  
  @Test
  public void alsoPersistAndRestore() throws JsonParseException, JsonMappingException, IOException{
    Plan p = getPlanWithZookeeperStorage();
    byte [] b = WorkerDao.serializePlan(p);
    Plan p1 = WorkerDao.deserializePlan(b);
    Assert.assertEquals(p1.getFeedDesc().getTheClass(), p.getFeedDesc().getTheClass());
    Assert.assertEquals(p1.getRootOperator().getTheClass(), p.getRootOperator()
            .getTheClass());
    Assert.assertEquals(p1.getOffsetStorageDesc().getOperatorClass() , p.getOffsetStorageDesc().getOperatorClass());
  }
  
  @Test
  public void persistAndRestore() throws JsonGenerationException, JsonMappingException, IOException {
    Plan p = getPlan();
    ObjectMapper om = new ObjectMapper();
    String asString = om.writeValueAsString(p);
    System.out.println(asString);
    Plan p1 = om.readValue(asString, Plan.class);
    Assert.assertEquals(p1.getFeedDesc().getTheClass(), p.getFeedDesc().getTheClass());
    Assert.assertEquals(p1.getRootOperator().getTheClass(), p.getRootOperator()
            .getTheClass());
  }
}
