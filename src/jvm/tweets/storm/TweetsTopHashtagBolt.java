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
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import tweets.storm.TopNTweetTopology;

import tweets.storm.tools.*;
import tweets.storm.tools.ContainsHashTags;
import tweets.storm.tools.Rankings;
import java.util.Map;
import java.util.Arrays;
import java.util.Set;

// This bolt filters the tweets that contain top hashtags.
public class TweetsTopHashtagBolt extends BaseRichBolt {
	// To output tuples from this bolt to the next stage bolts, if any
  OutputCollector collector;
  private Rankings rank;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // save the output collector for emitting tuples
    collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple)
  {
  	
  	// Get the component Id that created this tuple
  	String compId = tuple.getSourceComponent();

  	
  	// Check if tuple is coming from TotalRankingsBolt
  	if(TopNTweetTopology.TOTAL_RANKING_BOLT.equals(compId)){
  		rank = (Rankings)tuple.getValue(0);
  		return;
  	}
  	if(rank == null){
  		return;
  	}

  	// if the tuple is coming from TweetSpout.
  	String tweet = tuple.getString(0);
    Set<String> hashtags = ContainsHashTags.getHashtags(tweet);
    for(String hashtag:hashtags){
      for(Rankable r:rank.getRankings()){
        String tag = r.getObject().toString();
        if(hashtag.equals(tag)){
          collector.emit(new Values(hashtag,tweet));
        }
      }    
    }
  	
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    
    outputFieldsDeclarer.declare(new Fields("toptweets"));
  }

}