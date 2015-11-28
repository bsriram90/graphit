package edu.isu.coms.graphit.services;

import com.mongodb.DBObject;
import edu.isu.coms.graphit.repositories.RootTweetRepository;
import edu.isu.coms.graphit.repositories.TweetRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.ImmutableMap.of;

/**
 * Created by Naresh on 11/28/2015.
 */
@Service
public class RootTweetFinderService {


    @Autowired
    private TweetRepository tweetRepository;

    @Autowired
    private RootTweetRepository rootTweetRepository;

    public void run() {
        List<DBObject> retweetsToMap = tweetRepository.getTweets(100, of("root_finder_processed", false));
        while (retweetsToMap.size() > 0) {
            for (DBObject tweet : retweetsToMap) {
                if(isRootTweet(tweet)){
                    tweet.put("retweets",new ArrayList<>());
                    rootTweetRepository.save(tweet);
                }
                tweet.put("root_finder_processed", true);
                tweetRepository.save(tweet);
            }
            retweetsToMap = tweetRepository.getTweets(100, of("root_finder_processed", false));
        }

    }

    private boolean isRootTweet(DBObject tweet) {
        return tweet.get("retweet_id") == null && tweet.get("in_reply_to_status_id") == null
                && tweet.get("in_reply_to_user_id") == null;
    }
}
