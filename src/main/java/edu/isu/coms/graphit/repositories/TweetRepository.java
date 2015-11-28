package edu.isu.coms.graphit.repositories;

import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * Created by Naresh on 11/26/2015.
 */

@Repository
public class TweetRepository {

    @Resource(name ="tweets")
    private DBCollection tweets;

    public void save(DBObject tweet) {
        tweets.save(tweet);
    }

    public List<DBObject> getTweets(int size, Map<String, Object> filterParams) {
        BasicDBObject query = new BasicDBObject();
        for (String fieldName : filterParams.keySet()) {
            query.put(fieldName,filterParams.get(fieldName));
        }
        return tweets.find(query).limit(size).toArray();
    }
}
