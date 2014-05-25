package ufrj.dcc.sd.storm;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	
	Map<String, Integer> _counts = new HashMap<String, Integer>();
	OutputCollector _collector;
	
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}

	public void execute(Tuple tuple) {
		String word = tuple.getString(0);
		Integer count = _counts.get(word);
		if (count == null)
			count = 0;
		count++;
		_counts.put(word, count);
		_collector.emit(new Values(word, count));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

}
