package tweet;

import tweet.TwitterSpout;
import tweet.TwitterCounterLocation;
import tweet.TwitterHashTag;
import tweet.HashTagCount;
import bolts.FilterByHour;
import bolts.FilterByHourDate;
import bolts.TweetCountHour;
import bolts.EmitUser;
import bolts.EmitUserMentions;
import bolts.TweetCountHourDate;
import bolts.UserMentionsJoin;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TwitterCountTopology {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		// Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tweet-reader", new TwitterSpout());

		// Count tweets based on location
		builder.setBolt("location-counter", new TwitterCounterLocation(), 1)
				.shuffleGrouping("tweet-reader");

		// Count hashtags
		builder.setBolt("hashtag-emitter", new TwitterHashTag(), 5)
				.shuffleGrouping("tweet-reader");
		builder.setBolt("hashtag-counter", new HashTagCount(), 5)
				.fieldsGrouping("hashtag-emitter", new Fields("hashtag"));

		// Count tweets based on each hour
		builder.setBolt("filter-by-hour", new FilterByHour(), 5)
				.shuffleGrouping("tweet-reader");
		builder.setBolt("hourly-tweet-counter", new TweetCountHour())
				.fieldsGrouping("filter-by-hour", new Fields("hour"));

		// Perform Join between User name and tweets with same User-mention
		builder.setBolt("emit-user", new EmitUser()).shuffleGrouping(
				"tweet-reader");
		builder.setBolt("emit-user-mentions", new EmitUserMentions())
				.shuffleGrouping("tweet-reader");
		builder.setBolt("user-mentions-join", new UserMentionsJoin())
				.fieldsGrouping("emit-user", new Fields("tag"))
				.fieldsGrouping("emit-user-mentions", new Fields("tag"));

		// Count most number of tweets tweeted in an hour for the entire date
		// range
		builder.setBolt("filter-by-date-hour", new FilterByHourDate(), 5)
				.shuffleGrouping("tweet-reader");
		builder.setBolt("day-hour-tweet-counter", new TweetCountHourDate())
				.fieldsGrouping("filter-by-date-hour", new Fields("month-day","hour-period"));
		
		// Configuration
		Config conf = new Config();
		conf.put("TweetsFile", args[0]);
		conf.setDebug(false);

		// Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-With-Tweets", conf,
				builder.createTopology());
		Thread.sleep(1000);
		cluster.shutdown();
	}
}
