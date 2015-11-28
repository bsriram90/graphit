package edu.isu.coms.graphit.repositories;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;

import static java.lang.String.valueOf;

/**
 * Created by Naresh on 11/28/2015.
 */
@Repository
public class StatsRepository {

    @Resource(name = "stats")
    private DBCollection statsCollection;

    public void save(DBObject stat){
        statsCollection.save(stat);
    }

    public long getMissedRetweetsCount() {
        DBObject missedRetweetsQuery = new BasicDBObject();
        missedRetweetsQuery.put("stat_name", "missed_retweet_count");
        double stat_count = Double.parseDouble(valueOf(statsCollection.findOne(missedRetweetsQuery).get("stat_count")));
        return (long)stat_count;
    }

    public void updateMissedRetweetsCount(long missedRetweetCount) {
        DBObject missedRetweetsQuery = new BasicDBObject();
        missedRetweetsQuery.put("stat_name", "missed_retweet_count");
        DBObject missedRetweetStatObject = statsCollection.findOne(missedRetweetsQuery);
        missedRetweetStatObject.put("stat_count", missedRetweetCount);
        statsCollection.save(missedRetweetStatObject);
    }
}
