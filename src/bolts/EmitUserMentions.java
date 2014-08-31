package bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class EmitUserMentions implements IRichBolt{

	private OutputCollector collector;
	
	private String tag = "2";
	
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
		String user_name;
		String tweet = words[3];
		String location = words[0];
		
		int start_pos = 0;
		int end_pos = 0;
		int i = 0;

		// filter tweets that contain user mentions 
		while (i < tweet.length()) {
			if (tweet.charAt(i) == '@') {
				start_pos = i + 1;
				// System.out.println(start_pos);
				while ((tweet.charAt(i) != ' ') && (i < (tweet.length() - 1))) {
					i++;
					end_pos = i;
				}
				//System.out.println(start_pos + "," + end_pos);
				user_name= tweet.substring(start_pos, end_pos);
				
				//emit mentioned username and the tweet
				collector.emit(input, new Values(tag,user_name, tweet));
			}
			i++;
		}

		collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("tag","username","tweet"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
