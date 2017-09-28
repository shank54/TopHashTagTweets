package tweets.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import tweets.storm.tools.*;
import tweets.storm.tools.Rankings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

/**
 * A bolt that publishes top hash tags to redis
 */
public class ReportBolt extends BaseRichBolt
{
  // place holder to keep the connection to redis
  transient RedisConnection<String,String> redis;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // instantiate a redis connection
    RedisClient client = new RedisClient("localhost",6379);

    // initiate the actual connection
    redis = client.connect();
  }

  @Override
  public void execute(Tuple tuple)
  {

    Rankings rankableList = (Rankings) tuple.getValue(0);
    StringBuilder builder = new StringBuilder("0");
    for(Rankable r:rankableList.getRankings()){
        String word = r.getObject().toString();
        Long count = r.getCount();
        builder.append("|").append(word).append("|").append(count);
        redis.publish("TopTweetsTopology", builder.toString());
    }

    // // access the first column 'word'
    // String word = tuple.getStringByField("word");

    // // access the second column 'count'
    // Integer count = tuple.getIntegerByField("count");

    // // publish the word count to redis using word as the key
    // redis.publish("WordCountTopology", word + "|" + Long.toString(count));
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // nothing to add - since it is the final bolt
  }
}
