package edu.isu.coms.graphit.helpers;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Naresh on 11/30/2015.
 */
@Component
public class SolrToMongoDocTransformer {
    public List<DBObject> transform(SolrDocumentList searchresults) {
        List<DBObject> conversations =  new ArrayList<>();
        for (SolrDocument searchresult : searchresults) {
            DBObject conversation = new BasicDBObject();
            for (String solrField : searchresult.getFieldNames()) {
                if(!solrField.contains("_retweeted_")){
                    conversation.put(solrField,searchresult.get(solrField));
                }
            }
            conversations.add(conversation);
        }
        return conversations;
    }
}
