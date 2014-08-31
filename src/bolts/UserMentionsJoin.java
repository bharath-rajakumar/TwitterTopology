package bolts;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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

public class UserMentionsJoin implements IRichBolt {

	private OutputCollector collector;

	private String tweet = "";

	private Set<String> user_name = new TreeSet<String>();

	private String join_result = "";

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		Connection connection1 = null;
		PreparedStatement ps1 = null;

		String url = "jdbc:mysql://localhost:3306/Storm";
		String password = "password";
		String username = "root";

		try {
			Class.forName("com.mysql.jdbc.Driver");

			connection1 = DriverManager.getConnection(url, username, password);

			// check the number of fields in the tuple
			// if the tuple has two fields it comes from EmitUserMentions
			// else it comes from EmitUser
			user_name.add("add");
			if (input.size() == 3) {
				// System.out.println(input.getString(0)+">"+input.getString(1)+">"+input.getString(2));
				if (user_name.contains(input.getString(0))) {
					tweet = input.getString(2);
					join_result = input.getString(1) + ">>" + tweet;
					System.out.println(join_result);
					ps1 = connection1
							.prepareStatement("INSERT INTO Storm.user_mentions value (?,?)");
					ps1.setString(1, input.getString(1));
					ps1.setString(2, tweet);
					ps1.execute();
					collector.emit(new Values(join_result));
				} else {
					tweet = input.getString(2);
					user_name.add(input.getString(1));
					join_result = input.getString(1) + ">>" + tweet;
					System.out.println(join_result);
					ps1 = connection1
							.prepareStatement("INSERT INTO Storm.user_mentions value (?,?)");
					ps1.setString(1, input.getString(1));
					ps1.setString(2, tweet);
					ps1.execute();
					collector.emit(new Values(join_result));
				}

			} else {
				// System.out.println(input.getString(0)+">"+input.getString(1));
				if (!user_name.contains(input.getString(0))) {
					user_name.add(input.getString(0));
				}
			}
		} catch (SQLException e) {
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
	public void cleanup() {
		// TODO Auto-generated method stub

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
