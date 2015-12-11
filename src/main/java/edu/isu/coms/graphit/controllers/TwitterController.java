package edu.isu.coms.graphit.controllers;

import com.mongodb.*;
import edu.isu.coms.graphit.ApplicationEnvironment;
import edu.isu.coms.graphit.repositories.RootTweetSolrRepository;
import edu.isu.coms.graphit.repositories.TweetsSolrRepository;
import edu.isu.coms.graphit.services.*;
import org.apache.solr.client.solrj.SolrServerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;

/**
 * This is the main controller that is used to perform data collection and all steps required for conversation
 * detection.
 *
 * Created by Sriram on 26-11-2015.
 */
@Controller
@RequestMapping("/api/twitter")
public class TwitterController {

    @Resource(name="tweetsdump")
    private DBCollection tweetsDumpCollection;
    @Resource(name="searchmetadata")
    private DBCollection searchMetadataCollection;
    @Autowired
    private RootTweetSolrIndexer rootTweetSolrIndexer;
    @Autowired
    private ConversationDetectionService conversationDetectionService;
    @Autowired
    private TweetTransformationService tweetTransformationService;
    @Autowired
    private RootTweetFinderService rootTweetFinderService;
    @Autowired
    private RetweetMapperService retweetMapperService;
    @Autowired
    private RootTweetSolrRepository rootTweetSolrRepository;


    /**
     * This method behaves as the data collector. Upon calling '/api/twitter/timeline?searchString=<search-string>, the
     * method would query twitter 180 times on the search string every 15 minutes until no result is returned.
     *
     * @param searchString The search keyword to query twitter
     * @return
     */
    @RequestMapping(value = "/timeline", produces = MediaType.APPLICATION_JSON_VALUE, method = RequestMethod.GET)
    public @ResponseBody Object getTimelineTweets(@RequestParam("searchString") String searchString) {
        // the method returns false if there are no results. We replace all space with '+' for uniform encoding
        while(performRawTweetCollection(searchString.replace(" ", "+"))) {
            try {
                System.out.print("Sleeping for 15 mins");
                Thread.sleep(15 * 60 * 1000);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        return "Done";
    }

    /**
     * This method queries twitter using REST APIs. Each time a response is received, we store the tweets in the
     * 'tweetsDump' collection of MongoDB and we store the search related information in the 'searchMetadata' collection
     * of MongoDB. We read the least tweet_id and use that for subsequent queries to implement pagination.
     *
     * @param searchString The search keyword to query twitter
     * @return False if no results are returned, True otherwise
     */
    private boolean performRawTweetCollection(String searchString) {
        int count = 0;
        boolean noResults = false;
        // initialize rest templates and headers
        RestTemplate restTemplate = new RestTemplate();
        String queryString = "?q="+ searchString +"&count=100&result_type=recent&max_id=";
        // if the search string has already been used, continue from the last result
        Long max_id = findMaxIdForSearchString(searchString);
        String authToken = "Bearer AAAAAAAAAAAAAAAAAAAAAH7DiAAAAAAAzaHM6fVd%2BZoecdaOBoLVIg%2FnODY%3D6iSJg386lmd6KdmTx4DMLpqHQXmI4tQgZ5oYLZtCIj2tGLUBga";
        String timelineUrl = "https://api.twitter.com/1.1/search/tweets.json" + queryString;
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Authorization", authToken);
        HttpEntity<String> request = new HttpEntity<String>(headers);
        // Perform 180 requests as twitter allows that many requests every 15 mins.
        while(count++ < 180){
            String queryURL = timelineUrl;
            BulkWriteOperation bulk = tweetsDumpCollection.initializeUnorderedBulkOperation();
            // use max_id if present
            if (max_id != null){
                queryURL += max_id;
            } else {
                max_id = Long.MAX_VALUE;
            }
            System.out.println("Querying twitter - " + queryURL);
            ResponseEntity<Object> responseEntity =  restTemplate.exchange(queryURL, HttpMethod.GET, request, Object.class);
            LinkedHashMap<String,Object> results= (LinkedHashMap<String,Object>)responseEntity.getBody();
            // parse the reponse to find the tweets
            List tweets = (ArrayList)results.get("statuses");
            if(tweets.size() < 1) {
                // if there are no results, use this flag to end data collection on the topic
                noResults = true;
                break;
            }
            Iterator iterator = tweets.iterator();
            while(iterator.hasNext()){
                LinkedHashMap<String, Object> rawTweet = (LinkedHashMap<String, Object>) iterator.next();
                // processed flag is used to know if tweet has been indexed
                rawTweet.put("processed",false);
                BasicDBObject tweet = new BasicDBObject(rawTweet);
                // we keep track of th least id for pagination with Twitter
                if((Long)tweet.get("id") < max_id) {
                    max_id = (Long)tweet.get("id");
                }
                bulk.insert(tweet);
            }
            LinkedHashMap<String,Object> searchMetadata = (LinkedHashMap<String,Object>)results.get("search_metadata");
            searchMetadata.put("max_id", max_id);
            try {
                // we do untracked inserts to ignore duplicate tweets inserted into the database
                bulk.execute(new WriteConcern(0));
                // store search related information
                searchMetadataCollection.insert(new BasicDBObject(searchMetadata));
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        if(noResults) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * This method queries the searchMetadata to see if this search string has already been used before. If it has been
     * used, we would use the least id of the tweet returned to continue from this topic
     *
     * @param searchString
     * @return the minimum value of id for pagination
     */
    private Long findMaxIdForSearchString(String searchString) {
        String encodedSearchString = null;
        try {
            encodedSearchString = URLEncoder.encode(searchString, "UTF-8").replace("+", "%2B");
        } catch (UnsupportedEncodingException e){
            e.printStackTrace();
        }
        BasicDBObject query = new BasicDBObject("query", encodedSearchString);
        BasicDBObject fields = new BasicDBObject("max_id",1);
        BasicDBObject sort = new BasicDBObject("max_id",1);
        DBCursor cursor = searchMetadataCollection.find(query,fields).sort(sort).limit(1);
        while (cursor.hasNext()) {
            // this would only return one values, we find the max_id from that record.
            return (Long)cursor.next().get("max_id");
        }
        return null;
    }

    @RequestMapping(value = "/hello",method = RequestMethod.GET)
    public @ResponseBody String sayHello() {
        return "Hello";
    }

    @RequestMapping(value = "/transform",method = RequestMethod.GET)
    public @ResponseBody String transform() {
        tweetTransformationService.run();
        return "Done";
    }

    @RequestMapping(value = "/rootFinder", method = RequestMethod.GET)
    public @ResponseBody String findRoot() {
        rootTweetFinderService.run();
        return "Done";
    }

    @RequestMapping(value = "/map",method = RequestMethod.GET)
    public @ResponseBody String mapRetweets(){
        retweetMapperService.run();
        return "Done";
    }

    @RequestMapping(value = "/primaryHashtags",method = RequestMethod.GET)
    public @ResponseBody String getPrimaryHashtags(@RequestParam("keywords") String keywords){
        String[] keywordsArray = keywords.split(" ");
        System.out.println(rootTweetSolrRepository.getHashtagsForRootNodes(keywordsArray, 1));
        return "Done";
    }

    @RequestMapping(value = "/collectConversations",method = RequestMethod.GET)
    public @ResponseBody String collectConversations(@RequestParam("keywords") String keywords){
        conversationDetectionService.run(keywords);
        return "Done";
    }

    @RequestMapping(value = "/indexRootTweets",method = RequestMethod.GET)
    public @ResponseBody String indexRootTweets(){
        try {
            rootTweetSolrIndexer.run();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SolrServerException e) {
            e.printStackTrace();
            return e.getMessage();
        }
        return "Done";
    }


}
