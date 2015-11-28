package edu.isu.coms.graphit.repositories;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.Map;

/**
 * Created by Naresh on 11/26/2015.
 */

@Repository
public class TweetRepository {

    @Resource(name ="tweets")
    private DBCollection tweets;

    public void saveTweet(DBObject tweet) {
        tweets.save(tweet);
    }

}
