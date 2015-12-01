package edu.isu.coms.graphit.repositories;

import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

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
        rootTweetByIdQuery.put("id", retweet_id);
        return rootTweetsCollection.findOne(rootTweetByIdQuery);
    }

    public List<DBObject> getTweets(int size, Map<String, Object> filterParams) {
        BasicDBObject query = new BasicDBObject();
        for (String fieldName : filterParams.keySet()) {
            query.put(fieldName,filterParams.get(fieldName));
        }
        return rootTweetsCollection.find(query).limit(size).toArray();
    }

    public void updateIndexStatus(List<SolrInputDocument> tweetSolrDocs) {
        BasicDBList docIds = new BasicDBList();
        for (SolrInputDocument tweetSolrDoc : tweetSolrDocs) {
            docIds.add(tweetSolrDoc.get("id").getValue());
        }
        DBObject inClause = new BasicDBObject("$in", docIds);
        DBObject query = new BasicDBObject("id", inClause);
        rootTweetsCollection.updateMulti(query, new BasicDBObject( "$set", new BasicDBObject( "indexed", true) ));
    }
}
