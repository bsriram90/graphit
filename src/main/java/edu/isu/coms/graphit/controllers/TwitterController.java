package edu.isu.coms.graphit.controllers;

import com.mongodb.*;

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

    @RequestMapping(value = "/timeline", produces = MediaType.APPLICATION_JSON_VALUE, method = RequestMethod.GET)
    public @ResponseBody Object getTimelineTweets(@RequestParam("searchString") String searchString) {
        while(performRawTweetCollection(searchString)) {
            try {
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
        RestTemplate restTemplate = new RestTemplate();
        String encodedSearchString = null;
        try {
            encodedSearchString = URLEncoder.encode(searchString, "UTF-8").replace("+", "%2B");
        } catch (UnsupportedEncodingException e){
            e.printStackTrace();
        }
        String queryString = "?q="+ encodedSearchString +"&count=100&result_type=recent&max_id=";
        Long max_id = findMaxIdForSearchString(encodedSearchString);
        String authToken = "Bearer AAAAAAAAAAAAAAAAAAAAAH7DiAAAAAAAzaHM6fVd%2BZoecdaOBoLVIg%2FnODY%3D6iSJg386lmd6KdmTx4DMLpqHQXmI4tQgZ5oYLZtCIj2tGLUBga";
        String timelineUrl = "https://api.twitter.com/1.1/search/tweets.json" + queryString;
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Authorization", authToken);
        HttpEntity<String> request = new HttpEntity<String>(headers);
        while(count++ < 180){
            String queryURL = timelineUrl;
            if (max_id != null){
                queryURL += queryURL + max_id;
            }
            System.out.println("Querying twitter - " + queryURL);
            ResponseEntity<Object> responseEntity =  restTemplate.exchange(queryURL, HttpMethod.GET, request, Object.class);
            LinkedHashMap<String,Object> results= (LinkedHashMap<String,Object>)responseEntity.getBody();
            List<DBObject> rawTweets = new ArrayList<DBObject>();
            List<DBObject> searchMetadatas = new ArrayList<DBObject>();
            List tweets = (ArrayList)results.get("statuses");
            Iterator iterator = tweets.iterator();
            max_id = Long.MAX_VALUE;
            while(iterator.hasNext()){
                BasicDBObject tweet = new BasicDBObject((LinkedHashMap<String,Object>)iterator.next());
                if((Long)tweet.get("id") < max_id) {
                    max_id = (Long)tweet.get("id");
                }
                rawTweets.add(tweet);
            }
            if (rawTweets.size() < 1) {
                break;
            }
            LinkedHashMap<String,Object> searchMetadata = (LinkedHashMap<String,Object>)results.get("search_metadata");
            searchMetadata.put("max_id", max_id);
            searchMetadatas.add(new BasicDBObject(searchMetadata));
            try {
                if(rawTweets.size() > 0) {
                    tweetsDumpCollection.insert(rawTweets);
                    if(searchMetadatas.size() > 0) {
                        searchMetadataCollection.insert(searchMetadatas);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if(count == 180) {
            return true;
        } else {
            return false;
        }
    }

    private Long findMaxIdForSearchString(String encodedSearchString) {
        BasicDBObject query = new BasicDBObject("query", encodedSearchString);
        BasicDBObject fields = new BasicDBObject("max_id",1);
        BasicDBObject sort = new BasicDBObject("max_id",1);
        List<DBObject> myList = null;
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
}
