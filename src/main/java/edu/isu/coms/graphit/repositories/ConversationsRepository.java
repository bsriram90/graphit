package edu.isu.coms.graphit.repositories;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
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
        conversations.insert(conversationsTobeSaved);
    }

}
