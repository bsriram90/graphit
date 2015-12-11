package edu.isu.coms.graphit.helpers;

import com.mongodb.DBObject;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.springframework.stereotype.Component;

import java.util.Objects;

import static java.lang.String.valueOf;

/**
 * This object transforms a MongoDB DBObject to a Solr document by reading each fields in the DBObject and putting them
 * in a new Solr Document.
 *
 * Created by Naresh on 11/27/2015.
 */
@Component
public class SolrDocTransformer {
    public SolrInputDocument transform(DBObject tweet) {
        SolrInputDocument tweetSolrDocument = new SolrInputDocument();
        for (String key : tweet.keySet()) {
            if(!Objects.equals(key, "_id"))
                tweetSolrDocument.addField(key, tweet.get(key));
        }
        return tweetSolrDocument;
    }
}
