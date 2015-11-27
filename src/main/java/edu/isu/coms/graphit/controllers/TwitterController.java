package edu.isu.coms.graphit.controllers;

import com.mongodb.*;
import com.mongodb.util.JSON;
import edu.isu.coms.graphit.config.SpringConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
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
    public @ResponseBody Object getTimelineTweets() {
        RestTemplate restTemplate = new RestTemplate();
        String searchString = "starwars";
        String encodedSearchString = null;
        try {
            encodedSearchString = URLEncoder.encode(searchString, "UTF-8");
        } catch (UnsupportedEncodingException e){
            e.printStackTrace();
        }
        String queryString = "?q="+ encodedSearchString +"&count=100&result_type=popular";
        String authToken = "Bearer AAAAAAAAAAAAAAAAAAAAAH7DiAAAAAAAzaHM6fVd%2BZoecdaOBoLVIg%2FnODY%3D6iSJg386lmd6KdmTx4DMLpqHQXmI4tQgZ5oYLZtCIj2tGLUBga";
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Authorization", authToken);
        HttpEntity<String> request = new HttpEntity<String>(headers);
        while(queryString != null){
            String timelineUrl = "https://api.twitter.com/1.1/search/tweets.json" + queryString;
            System.out.println("Querying twitter - " + timelineUrl);
            ResponseEntity<Object> responseEntity =  restTemplate.exchange(timelineUrl, HttpMethod.GET, request, Object.class);
            LinkedHashMap<String,Object> results= (LinkedHashMap<String,Object>)responseEntity.getBody();
            List<DBObject> rawTweets = new ArrayList<DBObject>();
            List<DBObject> searchMetadatas = new ArrayList<DBObject>();
            List tweets = (ArrayList)results.get("statuses");
            Iterator iterator = tweets.iterator();
            while(iterator.hasNext()){
                rawTweets.add(new BasicDBObject((LinkedHashMap<String,Object>)iterator.next()));
            }
            LinkedHashMap<String,Object> searchMetadata = (LinkedHashMap<String,Object>)results.get("search_metadata");
            searchMetadatas.add(new BasicDBObject(searchMetadata));
            queryString = (String)searchMetadata.get("next_results");
            WriteResult writeResult1,writeResult2 = null;
            try {
                System.out.println("Writing batch. Next api call : " + queryString);
                if(rawTweets.size() > 0) {
                    writeResult1 = tweetsDumpCollection.insert(rawTweets);
                    System.out.println(writeResult1);
                    if(searchMetadatas.size() > 0) {
                        writeResult2 = searchMetadataCollection.insert(searchMetadatas);
                        System.out.println(writeResult2);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "Done";
    }

    @RequestMapping(value = "/hello",method = RequestMethod.GET)
    public @ResponseBody String sayHello() {
        return "Hello";
    }
}
