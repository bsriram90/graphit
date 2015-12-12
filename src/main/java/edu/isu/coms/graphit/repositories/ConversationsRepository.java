package edu.isu.coms.graphit.repositories;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.List;

/**
 * Created by Naresh on 11/30/2015.
 */
@Repository
public class ConversationsRepository {

    @Resource(name = "conversations")
    private DBCollection conversations;

    public void save(DBObject conversation){
        conversations.save(conversation);
    }

    public void save(List<DBObject> conversationsTobeSaved){
        if (conversationsTobeSaved.size()>0)
            conversations.insert(conversationsTobeSaved);
    }

    public void update(List<DBObject> conversationsTobeUpdated) {
        for (DBObject conversation : conversationsTobeUpdated) {
            conversations.save(conversation);
        }
    }

    public List<DBObject> getConversations() {
        BasicDBObject query = new BasicDBObject();
        return conversations.find(query).toArray();
    }

    public void reset(){
        conversations.remove(new BasicDBObject());
    }

}
