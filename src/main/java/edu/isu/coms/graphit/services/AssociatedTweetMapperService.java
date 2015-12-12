package edu.isu.coms.graphit.services;

import com.mongodb.DBObject;
import edu.isu.coms.graphit.repositories.ConversationsRepository;
import edu.isu.coms.graphit.repositories.RootTweetSolrRepository;
import org.apache.solr.common.SolrDocument;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.of;
import static java.lang.String.valueOf;

/**
 * Created by Naresh on 12/11/2015.
 */
@Service
public class AssociatedTweetMapperService {

    @Autowired
    private ConversationsRepository conversationsRepository;

    @Autowired
    private RootTweetSolrRepository rootTweetSolrRepository;


    public void run() {
        List<DBObject> conversations = conversationsRepository.getConversations();
        for (DBObject conversation : conversations) {
           List<SolrDocument> associatedTweets =  rootTweetSolrRepository.findAssociatedTweets(valueOf(conversation.get("text")), valueOf(conversation.get("url")),
                    valueOf(conversation.get("id")));
            List<Map> associatedTweetsInfo = new ArrayList<>();
            for (SolrDocument associatedTweet : associatedTweets) {
                associatedTweetsInfo.add(of("text",associatedTweet.get("text"),"id",associatedTweet.get("id"),
                        "url",associatedTweet.get("url")==null? "":associatedTweet.get("url")));
            }
            conversation.put("associated_tweets",associatedTweetsInfo);
        }
        conversationsRepository.update(conversations);
    }
}
