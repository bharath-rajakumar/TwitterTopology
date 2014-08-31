package tweet;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TwitterSpout implements IRichSpout {

	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed = false;
	private TopologyContext context;

	public boolean isDistributed() {
		return false;
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		try {
			this.context = context;
			this.fileReader = new FileReader(conf.get("TweetsFile").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error Reading file");
		}
		this.collector = collector;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		if (completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				System.out.println(e);
			}
		}

		String file_line;
		String[] string_split;
		String[] city = { "Manchester", "Tokyo", "Dubai", "Madras", "New York",
				"Bombay", "San Francisco", "Chicago", "Frankfurt", "Munich",
				"Lyon", "Madrid", "Barcelona" };
		BufferedReader reader = new BufferedReader(fileReader);
		try {
			// Read until end of file
			while ((file_line = reader.readLine()) != null) {
				// emit each line of the file
				// first split each fields based on comma (,)
				string_split = file_line.split(",");
				if (string_split.length == 6) {
					// append a random city as source of the tweet that is
					// getting emitted
					int idx = new Random().nextInt(city.length);
					String random_city = (city[idx]);
					String line_to_emit = random_city + "," + string_split[2]
							+ "," + string_split[4] + "," + string_split[5];
					this.collector.emit(new Values(line_to_emit), line_to_emit);
				}

			}
		} catch (Exception e) {
			throw new RuntimeException("Error Reading Tuple", e);
		} finally {
			completed = true;
		}
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		System.out.println("OK: " + msgId);
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		System.out.println("FAIL: " + msgId);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("line"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
