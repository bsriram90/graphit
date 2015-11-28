package edu.isu.coms.graphit.repositories;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * Created by Naresh on 11/27/2015.
 */
@Repository
public class TweetDumpRepository {

    @Resource(name = "tweetsdump")
    private DBCollection tweetsDump;

    public List getTweetsFromDump(int size , Map<String,Object> queryFields){
        BasicDBObject query = new BasicDBObject();
        for (String fieldName : queryFields.keySet()) {
            query.put(fieldName,queryFields.get(fieldName));
        }
        return tweetsDump.find(query).limit(size).toArray();
    }

    public void save(DBObject tweet){
        tweetsDump.save(tweet);
    }
}
