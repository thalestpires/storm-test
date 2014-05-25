package ufrj.dcc.sd.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class WordCountTopology {

	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("sentences", new RandomSentenceSpout(), 5);
		builder.setBolt("split", new SplitSentenceBolt(), 8)
			   .shuffleGrouping("sentences");
		builder.setBolt("count", new WordCountBolt(), 12)
			   .fieldsGrouping("split", new Fields("word"));

		Config config = new Config();
		config.setDebug(true);
		config.setNumWorkers(2);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", config, builder.createTopology());
		
		Utils.sleep(10000);
		cluster.killTopology("test");
		cluster.shutdown();
		
	}
}
