package edu.isu.coms.graphit.repositories;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Naresh on 11/30/2015.
 */
@Repository
public class RootTweetSolrRepository extends TweetsSolrRepository {

    @Autowired
    public RootTweetSolrRepository(SolrServer solrServer) {
        super(solrServer);
    }

    public List<SolrDocument> findConversations(List<String> hashtags, float acceptancePercentage) {
        SolrQuery query = new SolrQuery();
        SolrDocumentList results = new SolrDocumentList();
        List<SolrDocument> topResults = new ArrayList<>();
        String queryString = getQueryText(hashtags.toArray());
        query.set("q.alt", "text:" + queryString + ", hashtags:" + queryString);
        query.set("fq","retweet_count_in_dump : [50 TO *]");
        query.set("bf", "retweet_count_in_dump^0.2");
        query.set("fl","*,score");
        query.set("defType","dismax");
        query.set("rows",20000);
        try {
            QueryResponse response = solrServer.query(query);
            results = response.getResults();
            float acceptanceThreshold = response.getResults().getNumFound() * (acceptancePercentage / 100);
            int count = 0;

            for(SolrDocument doc : results) {
                if(count++ < acceptanceThreshold) {
                    topResults.add(doc);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return topResults;
    }

    public List<SolrDocument> findAssociatedTweets(String tweetText, String url, String converationId){
        SolrQuery query = new SolrQuery();
        SolrDocumentList results = new SolrDocumentList();
        List<SolrDocument> topAssociatedResults = new ArrayList<>();
        String queryString = getQuery(tweetText,url);
        query.set("q", queryString);
        query.set("fq","-id:"+converationId);
        try {
            QueryResponse response = solrServer.query(query);
            results = response.getResults();
            float acceptanceThreshold = response.getResults().getNumFound() * (20 / 100);
            int count = 0;
            for(SolrDocument doc : results) {
                if(count++ < 100) {
                    topAssociatedResults.add(doc);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return topAssociatedResults;
    }

    String getQuery(String tweet, String url) {
        return "text:(\""+ tweet +"\") OR "+ "urls:(\""+url+"\")";
    }
}
