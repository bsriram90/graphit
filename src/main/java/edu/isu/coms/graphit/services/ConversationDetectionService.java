package edu.isu.coms.graphit.services;

import edu.isu.coms.graphit.ApplicationEnvironment;
import edu.isu.coms.graphit.helpers.SolrToMongoDocTransformer;
import edu.isu.coms.graphit.repositories.ConversationsRepository;
import edu.isu.coms.graphit.repositories.ResultsRepository;
import edu.isu.coms.graphit.repositories.RootTweetSolrRepository;
import org.apache.solr.common.SolrDocumentList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;

import static com.google.common.collect.ImmutableMap.of;

/**
 * Created by Naresh on 11/30/2015.
 */
@Service
public class ConversationDetectionService {

    @Autowired
    private RootTweetSolrRepository rootTweetSolrRepository;

    @Autowired
    private ApplicationEnvironment applicationEnvironment;

    @Autowired
    private ResultsRepository resultsRepository;

    @Autowired
    private ConversationsRepository conversationsRepository;

    @Autowired
    private SolrToMongoDocTransformer solrToMongoDocTransformer;

    public void run(String keywords) {
        String[] keywordsArray = keywords.split(" ");
        List<String> relevantHashtags = rootTweetSolrRepository.getHashtagsForRootNodes(keywordsArray, applicationEnvironment.getFacetAcceptanceThreshold());
        resultsRepository.updateRelevantHashTags(of("keywords",keywords,"name","relevantHashtags","time",new Date(),"value",relevantHashtags));
        SolrDocumentList searchresults =  rootTweetSolrRepository.findRootTweets(relevantHashtags, applicationEnvironment.getConversationsFilterLimit());
        conversationsRepository.save(solrToMongoDocTransformer.transform(searchresults));
    }
}
