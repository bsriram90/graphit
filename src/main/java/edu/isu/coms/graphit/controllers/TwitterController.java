package edu.isu.coms.graphit.controllers;

import org.springframework.http.*;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestTemplate;

/**
 * Created by Sriram on 26-11-2015.
 */
@Controller
@RequestMapping("/api/twitter")
public class TwitterController {
    @RequestMapping(value = "/timeline", produces = MediaType.APPLICATION_JSON_VALUE, method = RequestMethod.GET)
    public @ResponseBody Object getTimelineTweets() {
        RestTemplate restTemplate = new RestTemplate();
        String timelineUrl = "https://api.twitter.com/1.1/search/tweets.json?q=modiinus&since_id=654044701524254720";
        String authToken = "Bearer AAAAAAAAAAAAAAAAAAAAAH7DiAAAAAAAzaHM6fVd%2BZoecdaOBoLVIg%2FnODY%3D6iSJg386lmd6KdmTx4DMLpqHQXmI4tQgZ5oYLZtCIj2tGLUBga";
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Authorization", authToken);
        HttpEntity<String> request = new HttpEntity<String>(headers);
        ResponseEntity<Object> responseEntity =  restTemplate.exchange(timelineUrl, HttpMethod.GET, request, Object.class);
        Object results= responseEntity.getBody();
        return results;
    }

    @RequestMapping(value = "/hello",method = RequestMethod.GET)
    public @ResponseBody String sayHello() {
        return "Hello";
    }
}
