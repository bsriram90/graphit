package edu.isu.coms.graphit.helpers;

import com.mongodb.DBObject;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Created by Naresh on 11/29/2015.
 */
@Component
public class RootTweetSolrTransformer {

    // list of fields to be discarded
    private String[] unwantedFields = new String[]{"in_reply_to_status_id","in_reply_to_user_id","in_reply_to_screen_name","root_finder_processed","retweet_mapper_processed","indexed"};

    @Autowired
    private SolrDocTransformer solrDocTransformer;

    /**
     * This method takes a MongoDB DBObject that represents a tweet, strips unwanted field and returns a Solr Document.
     *
     * @param root_tweet
     * @return a Solr document
     */
    public SolrInputDocument transform(DBObject root_tweet){
        // remove unnecessary fields
        for (String unwantedField : unwantedFields) {
            root_tweet.removeField(unwantedField);
        }
        List<Map<String,Object>> retweets = (List<Map<String, Object>>) root_tweet.get("retweets");
        int retweetIndex=0;
        // store retweet related information in the same document
        for (Map<String, Object> retweet : retweets) {
            ++retweetIndex;
            root_tweet.put(retweetIndex + "_retweeted_id",retweet.get("retweet_id"));
            root_tweet.put(retweetIndex + "_retweeted_user_id",retweet.get("user_id"));
        }
        root_tweet.put("retweet_count_in_dump",retweets.size());
        return solrDocTransformer.transform(root_tweet);
    }
}