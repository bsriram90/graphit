package edu.isu.coms.graphit.repositories;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;

/**
 * Created by Naresh on 11/28/2015.
 */
@Repository
public class RootTweetRepository {

    @Resource(name = "roottweets")
    private DBCollection rootTweetsCollection;

    public void save(DBObject tweet){
        rootTweetsCollection.save(tweet);
    }

    public DBObject find(Object retweet_id) {
        DBObject rootTweetByIdQuery = new BasicDBObject();
        rootTweetByIdQuery.put("id",retweet_id);
        return rootTweetsCollection.findOne(rootTweetByIdQuery);
    }
}
