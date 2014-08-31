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

public class TweetCountHourDate implements IRichBolt{

	private OutputCollector collector;

	private HashMap<String, Integer> hourdate_count = new HashMap<String, Integer>();
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String month_date = input.getString(0);
		String hour = input.getString(1);
		String month_hour_date = month_date+","+hour;

		if (hourdate_count.containsKey(month_hour_date)) {
			Integer c = hourdate_count.get(month_hour_date) + 1;
			hourdate_count.put(month_hour_date, c);
		} else {
			hourdate_count.put(month_hour_date, 1);
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
			
			for (Map.Entry<String, Integer> entry : hourdate_count.entrySet()) {
				ps1 = connection1.prepareStatement("INSERT INTO Storm.tweets_by_date_hour value (?,?,?)");
				String[] date_time = entry.getKey().split(",");
				ps1.setString(1, date_time[0]);
				ps1.setString(2, date_time[1]);
				ps1.setInt(3, entry.getValue());
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
