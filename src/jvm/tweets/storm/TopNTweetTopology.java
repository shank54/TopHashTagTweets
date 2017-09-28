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

class TopNTweetTopology
{
  public static String TOTAL_RANKING_BOLT = "total-ranking";

  public static void main(String[] args) throws Exception
  {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    // 
    int TOP_N = 1000;

    // Twitter credentials
    TweetSpout tweetSpout = new TweetSpout(
        "Your Consumer Key",
        "Your Consumer Secret Key",
        "Your Access Token",
        "Your Access Token Secret"
    );

    // tweet spout with parallelism of 1
    builder.setSpout("tweet-spout", tweetSpout, 1);

    // Clean tweet bolt with parallelism of 10 with shuflle grouping
    builder.setBolt("clean-tweet-bolt", new CleanTweetsBolt(), 10).shuffleGrouping("tweet-spout");

    // Parse tweet bolt using shuffle grouping
    builder.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 10).shuffleGrouping("clean-tweet-bolt");

    // attach the count bolt using fields grouping - parallelism of 15
    builder.setBolt("count-bolt", new CountBolt(), 15).fieldsGrouping("parse-tweet-bolt", new Fields("tweet-word"));

    // Intermediate Ranking Bolt with fields grouping
    builder.setBolt("intermediate-ranking",new IntermediateRankingsBolt(TOP_N),4).fieldsGrouping("count-bolt",new Fields("word"));

    // Total Ranking Bolt with fields grouping
    builder.setBolt("total-ranking",new TotalRankingsBolt(TOP_N)).globalGrouping("intermediate-ranking");

    // report bolt
    builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("total-ranking");

    //Top Tweets bolt
    builder.setBolt("top-tweets",new TweetsTopHashtagBolt()).globalGrouping("total-ranking");

    //report top tweets bolt
    builder.setBolt("report-toptweets-bolt", new ReportTopTweetsBolt(), 1).globalGrouping("top-tweets");

    // create the default config object
    Config conf = new Config();

    // set the config in debugging mode
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers(3);

      // create the topology and submit with config
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } else {

      // run it in a simulated local cluster

      // set the number of threads to run - similar to setting number of workers in live cluster
      conf.setMaxTaskParallelism(3);

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      cluster.submitTopology("tweet-word-count", conf, builder.createTopology());

      // let the topology run for 300 seconds. note topologies never terminate!
      Utils.sleep(300000);

      // now kill the topology
      cluster.killTopology("tweet-word-count");

      // we are done, so shutdown the local cluster
      cluster.shutdown();
    }
  }
}
