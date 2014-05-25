package ufrj.dcc.sd.storm;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitSentenceBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	
	OutputCollector _collector;

	public SplitSentenceBolt() {
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}

	public void execute(Tuple input) {
		String sentence = input.getString(0);
		String[] split = sentence.split(" ");
		for (String word : split) {
			_collector.emit(new Values(word));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
	

}
