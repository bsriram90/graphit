package edu.isu.coms.graphit.controllers;

import com.mongodb.*;
import edu.isu.coms.graphit.repositories.SolrRepository;
import edu.isu.coms.graphit.services.RetweetMapperService;
import edu.isu.coms.graphit.services.RootTweetFinderService;
import edu.isu.coms.graphit.services.TweetTransformationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Resource;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Created by Sriram on 26-11-2015.
 */
@Controller
@RequestMapping("/api/twitter")
public class TwitterController {

    @Resource(name="tweetsdump")
    private DBCollection tweetsDumpCollection;
    @Resource(name="searchmetadata")
    private DBCollection searchMetadataCollection;
    @Resource(name="roottweets")
    private DBCollection rootTweetsCollection;
    @Resource(name="conversations")
    private DBCollection conversationsCollection;


    @Autowired
    private TweetTransformationService tweetTransformationService;

    @Autowired
    private RootTweetFinderService rootTweetFinderService;

    @Autowired
    private RetweetMapperService retweetMapperService;

    @Autowired
    private SolrRepository solrRepository;

    @RequestMapping(value = "/timeline", produces = MediaType.APPLICATION_JSON_VALUE, method = RequestMethod.GET)
    public @ResponseBody Object getTimelineTweets(@RequestParam("searchString") String searchString) {
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

    private boolean performRawTweetCollection(String searchString) {
        int count = 0;
        boolean noResults = false;
        RestTemplate restTemplate = new RestTemplate();
        String queryString = "?q="+ searchString +"&count=100&result_type=recent&max_id=";
        Long max_id = findMaxIdForSearchString(searchString);
        String authToken = "Bearer AAAAAAAAAAAAAAAAAAAAAH7DiAAAAAAAzaHM6fVd%2BZoecdaOBoLVIg%2FnODY%3D6iSJg386lmd6KdmTx4DMLpqHQXmI4tQgZ5oYLZtCIj2tGLUBga";
        String timelineUrl = "https://api.twitter.com/1.1/search/tweets.json" + queryString;
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Authorization", authToken);
        HttpEntity<String> request = new HttpEntity<String>(headers);
        while(count++ < 180){
            String queryURL = timelineUrl;
            BulkWriteOperation bulk = tweetsDumpCollection.initializeUnorderedBulkOperation();
            if (max_id != null){
                queryURL += max_id;
            } else {
                max_id = Long.MAX_VALUE;
            }
            System.out.println("Querying twitter - " + queryURL);
            ResponseEntity<Object> responseEntity =  restTemplate.exchange(queryURL, HttpMethod.GET, request, Object.class);
            LinkedHashMap<String,Object> results= (LinkedHashMap<String,Object>)responseEntity.getBody();
            List tweets = (ArrayList)results.get("statuses");
            if(tweets.size() < 1) {
                noResults = true;
                break;
            }
            Iterator iterator = tweets.iterator();
            while(iterator.hasNext()){
                LinkedHashMap<String, Object> rawTweet = (LinkedHashMap<String, Object>) iterator.next();
                rawTweet.put("processed",false);
                BasicDBObject tweet = new BasicDBObject(rawTweet);
                if((Long)tweet.get("id") < max_id) {
                    max_id = (Long)tweet.get("id");
                }
                bulk.insert(tweet);
            }
            LinkedHashMap<String,Object> searchMetadata = (LinkedHashMap<String,Object>)results.get("search_metadata");
            searchMetadata.put("max_id", max_id);
            try {
                bulk.execute(new WriteConcern(0));
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
        System.out.println(solrRepository.getHashtagsForRootNodes(keywordsArray, 10));
        return "Done";
    }

    @RequestMapping(value = "/collectConversations",method = RequestMethod.GET)
    public @ResponseBody String collectConversations(@RequestParam("keywords") String keywords){
        String[] keywordsArray = keywords.split(" ");
        List<String> popularHashtags = solrRepository.getHashtagsForRootNodes(keywordsArray, 50);
        BasicDBObject query = new BasicDBObject("hashtags", popularHashtags);
        DBCursor cursor = rootTweetsCollection.find(query);
        List<DBObject> rootTweets = cursor.toArray();
        conversationsCollection.insert(rootTweets);
        return "Done";
    }
}
