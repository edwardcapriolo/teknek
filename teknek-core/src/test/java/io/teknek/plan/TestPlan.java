package io.teknek.plan;

import io.teknek.driver.Minus1Operator;
import io.teknek.driver.TestDriver;
import io.teknek.driver.Times2Operator;
import io.teknek.feed.FixedFeed;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;
import io.teknek.util.MapBuilder;

import java.io.IOException;

import junit.framework.Assert;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;



public class TestPlan {

  public static Plan getPlan(){
    Plan p = new Plan().withFeedDesc(
            new FeedDesc().withFeedClass(FixedFeed.class.getName()).withProperties(
                    MapBuilder.makeMap(FixedFeed.NUMBER_OF_PARTITIONS, 2, FixedFeed.NUMBER_OF_ROWS,
                            10))).withRootOperator(
            new OperatorDesc(new Minus1Operator()).withNextOperator(new OperatorDesc(
                    new Times2Operator())));
    return p;
  }
  
  @Test
  public void persistAndRestore() throws JsonGenerationException, JsonMappingException, IOException {
    Plan p = getPlan();
    ObjectMapper om = new ObjectMapper();
    String asString = om.writeValueAsString(p);
    Plan p1 = om.readValue(asString, Plan.class);
    Assert.assertEquals(p1.getFeedDesc().getFeedClass(), p.getFeedDesc().getFeedClass());
    Assert.assertEquals(p1.getRootOperator().getOperatorClass(), p.getRootOperator()
            .getOperatorClass());
  }
}
