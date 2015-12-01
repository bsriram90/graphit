package edu.isu.coms.graphit.services;

import com.mongodb.DBObject;
import edu.isu.coms.graphit.helpers.RootTweetSolrTransformer;
import edu.isu.coms.graphit.repositories.RootTweetRepository;
import edu.isu.coms.graphit.repositories.RootTweetSolrRepository;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.ImmutableMap.of;

/**
 * Created by Naresh on 11/29/2015.
 */
@Service
public class RootTweetSolrIndexer {

    @Autowired
    private RootTweetRepository rootTweetRepository;

    @Autowired
    private RootTweetSolrTransformer rootTweetSolrTransformer;

    @Autowired
    private RootTweetSolrRepository rootTweetSolrRepository;

    public void run() throws IOException, SolrServerException {

        List<DBObject> rootTweetsTobeIndexed = rootTweetRepository.getTweets(1000, of("indexed", false));
        List<SolrInputDocument> tweetSolrDocs =  new ArrayList<>();
        while (rootTweetsTobeIndexed.size() > 0) {
            for (DBObject tweet : rootTweetsTobeIndexed) {
                tweetSolrDocs.add(rootTweetSolrTransformer.transform(tweet));
                tweet.put("indexed", true);
            }
            rootTweetSolrRepository.index(tweetSolrDocs);
            rootTweetRepository.updateIndexStatus(tweetSolrDocs);
            rootTweetsTobeIndexed = rootTweetRepository.getTweets(1000, of("indexed", false));
        }


    }
}

