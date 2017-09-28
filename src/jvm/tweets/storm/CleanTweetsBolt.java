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
import backtype.storm.topology.BasicOutputCollector;
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

//A bolt that removes the tweets which doen't contain any hashtag.
public class CleanTweetsBolt extends BaseBasicBolt {

	@Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Set<String> hashtags = ContainsHashTags.getHashtags(tuple.getString(0));
        if (hashtags.size() == 0) {
            return;
        }
        String tweet = tuple.getString(0);

        collector.emit(new Values(tweet));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
}