package edu.isu.coms.graphit.helpers;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.valueOf;

/**
 * Created by Naresh on 11/26/2015.
 */
@Component
public class TweetTransformer {
    /**
     * This method transforms a raw tweet to a processed tweet. We map the core, user, hashtags, urls, user mentions
     * and retweet details
     *
     * @param rawTweet
     * @return a processed tweet
     */
    public static DBObject transform(DBObject rawTweet) {
        DBObject transformedTweet = new BasicDBObject();
        Map<String, Object> entitiesObject = (Map<String, Object>) rawTweet.get("entities");
        mapMainFields(rawTweet, transformedTweet);
        mapUserFields(rawTweet, transformedTweet);
        mapHashtags(transformedTweet, entitiesObject);
        mapUrls(transformedTweet, entitiesObject);
        mapUserMentions(transformedTweet, entitiesObject);
        mapRetweetDetails(rawTweet, transformedTweet);
        return transformedTweet;
    }

    /**
     * Read all retweets in the raw tweet and add them as a single field in the transformed tweet
     *
     * @param rawTweet
     * @param transformedTweet
     */
    private static void mapRetweetDetails(DBObject rawTweet, DBObject transformedTweet) {
        Map<String,Object> retweetObject = (Map<String, Object>) rawTweet.get("retweeted_status");
        if(retweetObject!=null) {
            transformedTweet.put("retweet_id", retweetObject.get("id"));
        }
    }

    /**
     * Read all user mentions in the raw tweet and add them as a single field in the transformed tweet
     *
     * @param entitiesObject
     * @param transformedTweet
     */
    private static void mapUserMentions(DBObject transformedTweet, Map<String, Object> entitiesObject) {
        List<String> userMentions = new ArrayList<>();
        List<Map<String, Object>> userMentionsObject = (List<Map<String, Object>>) entitiesObject.get("user_mentions");
        for (Map<String, Object> userMentionObject : userMentionsObject) {
            userMentions.add(valueOf(userMentionObject.get("id_str")));
        }
        transformedTweet.put("user_mentions", userMentions);
    }

    /**
     * Read all URLs in the raw tweet and add them as a single field in the transformed tweet
     *
     * @param entitiesObject
     * @param transformedTweet
     */
    private static void mapUrls(DBObject transformedTweet, Map<String, Object> entitiesObject) {
        List<Map<String, Object>> urlsObject = (List<Map<String, Object>>) entitiesObject.get("urls");
        List<String> urls = new ArrayList<>();
        for (Map<String, Object> urlObject : urlsObject) {
            urls.add(valueOf(urlObject.get("expanded_url")));
        }
        transformedTweet.put("urls", urls);
    }

    /**
     * Read all hashtags in the raw tweet and add them as a single field in the transformed tweet
     *
     * @param entitiesObject
     * @param transformedTweet
     */
    private static void mapHashtags(DBObject transformedTweet, Map<String, Object> entitiesObject) {
        List<Map<String, Object>> hashtagObjects = (List<Map<String, Object>>) entitiesObject.get("hashtags");
        List<String> hashtags = new ArrayList<>();
        for (Map<String, Object> hashtagObject : hashtagObjects) {
            hashtags.add(valueOf(hashtagObject.get("text")));
        }
        transformedTweet.put("hashtags", hashtags);
    }

    /**
     * Read all core fields in the raw tweet and add them as a single field in the transformed tweet.
     * Core fields include ID, Text, Is reply, retweet_count and favourite_count
     *
     * @param rawTweet
     * @param transformedTweet
     */
    private static void mapMainFields(DBObject rawTweet, DBObject transformedTweet) {
        String[] mainFields = new String[]{"id","text", "in_reply_to_status_id", "in_reply_to_user_id",
                "in_reply_to_screen_name","retweet_count","favorite_count"};
        for (String mainField : mainFields) {
            transformedTweet.put(mainField, rawTweet.get(mainField));
        }
        transformedTweet.put("created_at", valueOf(rawTweet.get("created_at")));
    }

    /**
     * Read all user fields in the raw tweet and add them as a single field in the transformed tweet
     *
     * @param rawTweet
     * @param transformedTweet
     */
    private static void mapUserFields(DBObject rawTweet, DBObject transformedTweet) {
        String[] userFields = new String[]{"id", "name", "screen_name", "followers_count", "friends_count",
                "statuses_count"};
        Map<String, Object> userObject = (Map<String, Object>) rawTweet.get("user");
        for (String userField : userFields) {
            Object userFieldValue = userObject.get(userField);
            if (userFieldValue != null)
                transformedTweet.put("user_" + userField, userFieldValue);
        }
    }
}
