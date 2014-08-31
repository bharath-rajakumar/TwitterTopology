package bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FilterByHourDate implements IRichBolt{

	private OutputCollector collector;
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
		String[] date_time = words[1].split(" ");
		String month = date_time[1];
		String day = date_time[2];
		String[] time = date_time[3].split(":");
		int start_hour = Integer.parseInt(time[0]);
		int end_hour;
		if(start_hour == 23) {
			end_hour = 0;
		} else {
			end_hour = start_hour + 1;
		}
		String hour_period = start_hour +":00 "+"- "+end_hour+":00 ";
		String month_day = month+" - "+day;
		//emit the hour
		//System.out.println(hour_period);
		collector.emit(input, new Values(month_day,hour_period));
		
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("month-day","hour-period"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
