package edu.isu.coms.graphit.repositories;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

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
    public SolrDocumentList findRootTweets(List<String> hashtags, float acceptancePercentage) {
        SolrQuery query = new SolrQuery();
        SolrDocumentList results = new SolrDocumentList();
        String queryString = getQueryText(hashtags.toArray());
        query.set("q.alt", "text:" + queryString + ", hashtags:" + queryString);
        query.set("fq","retweet_count_in_dump : [50 TO *]");
        query.set("bf", "retweet_count_in_dump^0.2");
        query.set("defType","dismax");
        try {
            QueryResponse response = solrServer.query(query);
            results = response.getResults();
            float acceptanceThreshold = response.getResults().getNumFound() * (acceptancePercentage / 100);
            int count = 0;
            for(SolrDocument doc : results) {
                if(count++ < acceptanceThreshold) {
                    doc.setField("in_conversation",true);
                    // TODO : use different repo
                    this.solrServer.add(ClientUtils.toSolrInputDocument(doc));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }
}
