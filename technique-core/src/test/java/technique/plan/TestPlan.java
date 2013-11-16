package technique.plan;

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

import technique.driver.TestDriver;
import technique.feed.FixedFeed;


public class TestPlan {

  @Test
  public void persistAndRestore() throws JsonGenerationException, JsonMappingException, IOException {
    Plan p = new Plan().withFeedDesc( new FeedDesc()
      .withFeedClass(FixedFeed.class.getName())
        .withProperties(MapBuilder.makeMap(
                FixedFeed.NUMBER_OF_PARTITIONS, 2, 
                FixedFeed.NUMBER_OF_ROWS, 10)
        )
      ).withRootOperator(new OperatorDesc(TestDriver.times2Operator())
        .withNextOperator(new OperatorDesc(TestDriver.minus1Operator()))
    );
    ObjectMapper om = new ObjectMapper();
    String asString = om.writeValueAsString(p);
    Plan p1 = om.readValue(asString, Plan.class);
    Assert.assertEquals(p1.getFeedDesc().getFeedClass(), p.getFeedDesc().getFeedClass());
    Assert.assertEquals(p1.getRootOperator().getOperatorClass(), p.getRootOperator()
            .getOperatorClass());
  }
}
