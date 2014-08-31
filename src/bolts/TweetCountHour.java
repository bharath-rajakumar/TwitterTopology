package bolts;

import java.util.HashMap;
import java.util.Map;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TweetCountHour implements IRichBolt{

	private OutputCollector collector;
	
	private HashMap<String, Integer> hourly_count = new HashMap<String, Integer>();
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String hour = input.getString(0);

		if (hourly_count.containsKey(hour)) {
			Integer c = hourly_count.get(hour) + 1;
			hourly_count.put(hour, c);
		} else {
			hourly_count.put(hour, 1);
		}
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		Connection connection1 = null;
		PreparedStatement ps1 = null;
		
		String url = "jdbc:mysql://localhost:3306/Storm";
		String password = "password";
		String username = "root";
		
		try
		{
			Class.forName("com.mysql.jdbc.Driver");
			
			connection1 = DriverManager.getConnection(url ,username , password );
			
			for (Map.Entry<String, Integer> entry : hourly_count.entrySet()) {
				ps1 = connection1.prepareStatement("INSERT INTO Storm.tweets_by_hour value (?,?)");
				ps1.setString(1, entry.getKey());
				ps1.setInt(2, entry.getValue());
				ps1.execute();
			}
		} catch(SQLException e) {
			System.out.println(e);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				connection1.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
		/*
		for (Map.Entry<String, Integer> entry : hourly_count.entrySet()) {
			System.out.println(entry.getKey() + "," + entry.getValue());
		}
		*/
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
