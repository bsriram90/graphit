package edu.isu.coms.graphit.services;

import com.mongodb.DBObject;
import edu.isu.coms.graphit.helpers.SolrDocTransformer;
import edu.isu.coms.graphit.helpers.TweetTransformer;
import edu.isu.coms.graphit.repositories.SolrRepository;
import edu.isu.coms.graphit.repositories.TweetDumpRepository;
import edu.isu.coms.graphit.repositories.TweetRepository;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.ImmutableMap.of;

/**
 * Created by Naresh on 11/26/2015.
 */

@Service
public class TweetTransformationService {

    @Autowired
    private TweetRepository tweetRepository;

    @Autowired
    private TweetDumpRepository tweetDumpRepository;

    @Autowired
    private SolrRepository solrRepository;

    @Autowired
    private TweetTransformer tweetTransformer;

    @Autowired
    private SolrDocTransformer solrDocTransformer;

    public void run() {
        List<DBObject> tweetsFromDump = tweetDumpRepository.getTweetsFromDump(100, of("processed", false));
        long transformedTweetCounter =1;
        while (tweetsFromDump.size() > 0) {
            try {
                List<SolrInputDocument> tweetSolrDocs = new ArrayList<>();
                for (DBObject rawTweet : tweetsFromDump) {
                    DBObject transformedTweet = tweetTransformer.transform(rawTweet);
                    tweetRepository.save(transformedTweet);
                    tweetSolrDocs.add(solrDocTransformer.transform(transformedTweet));
                    rawTweet.put("processed", true);
                    tweetDumpRepository.save(rawTweet);
                    transformedTweetCounter++;
                }
                solrRepository.index(tweetSolrDocs);
                System.out.println("Total Tweets transformed: "+ transformedTweetCounter);
                tweetsFromDump = tweetDumpRepository.getTweetsFromDump(100, of("processed", false));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SolrServerException e) {
                e.printStackTrace();
            }
        }
    }


}
