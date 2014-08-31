package tweet;

import java.util.ArrayList;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TwitterHashTag implements IRichBolt {

	private OutputCollector collector;

	private ArrayList<String> hashtag_list = new ArrayList<String>();

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String sentence = input.getString(0);
		String[] words = sentence.split(",");
		String tweet = words[3];
		String location = words[0];
		
		int start_pos = 0;
		int end_pos = 0;
		int i = 0;

		// filter tweets that are tweeted from given location
		while (i < tweet.length()) {
			if (tweet.charAt(i) == '#') {
				start_pos = i + 1;
				// System.out.println(start_pos);
				while ((tweet.charAt(i) != ' ') && (i < (tweet.length() - 1))) {
					i++;
					end_pos = i;
				}
				//System.out.println(start_pos + "," + end_pos);
				String hashtag = tweet.substring(start_pos, end_pos);
				hashtag_list.add(hashtag);
				collector.emit(input, new Values(hashtag));
			}
			i++;
		}

		collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		for(String h: hashtag_list) {
			System.out.println(h);
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("hashtag"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
