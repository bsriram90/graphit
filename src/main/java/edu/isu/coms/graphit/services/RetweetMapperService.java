package edu.isu.coms.graphit.services;

import com.mongodb.DBObject;
import edu.isu.coms.graphit.repositories.RootTweetRepository;
import edu.isu.coms.graphit.repositories.StatsRepository;
import edu.isu.coms.graphit.repositories.TweetRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.of;

/**
 * Created by Naresh on 11/28/2015.
 */
@Service
public class RetweetMapperService {

    @Autowired
    private TweetRepository tweetRepository;

    @Autowired
    private RootTweetRepository rootTweetRepository;

    @Autowired
    private StatsRepository statsRepository;

    public void run(){
        List<DBObject> retweetsTobeMapped = tweetRepository.getTweets(100, of("retweet_mapper_processed", false));
        long missedRetweetCount = statsRepository.getMissedRetweetsCount();
        while (retweetsTobeMapped.size() > 0) {
            for (DBObject tweet : retweetsTobeMapped) {
                DBObject rootTweet = rootTweetRepository.find(tweet.get("retweet_id"));
                if(rootTweet != null){
                    List<Map<String,Object>> retweets = (List<Map<String, Object>>) rootTweet.get("retweets");
                    retweets.add(of("retweet_id",tweet.get("id"),"user_id",tweet.get("user_id")));
                    rootTweet.put("retweets",retweets);
                    rootTweetRepository.save(rootTweet);
                }
                else{

                    tweet.put("root_tweet_not_found", true);
                }
                missedRetweetCount++;
                tweet.put("retweet_mapper_processed", true);
                tweetRepository.save(tweet);
            }
            retweetsTobeMapped = tweetRepository.getTweets(100, of("retweet_mapper_processed", false));
        }
        statsRepository.updateMissedRetweetsCount(missedRetweetCount);
    }
}
