package edu.isu.coms.graphit.repositories;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.Date;
import java.util.Map;

/**
 * Created by Naresh on 11/30/2015.
 */
@Repository
public class ResultsRepository {

    @Resource(name = "results")
    private DBCollection resultsCollection;

    public void save(DBObject result) {
        resultsCollection.save(result);
    }

    public void updateRelevantHashTags(Map<String,Object> results) {
        resultsCollection.remove(new BasicDBObject("name", "relevantHashtags"));
        resultsCollection.save(new BasicDBObject(results));
    }
}
