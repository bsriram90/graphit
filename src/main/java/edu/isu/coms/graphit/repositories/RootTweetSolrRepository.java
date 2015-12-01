package edu.isu.coms.graphit.repositories;

import org.apache.solr.client.solrj.SolrServer;

/**
 * Created by Naresh on 11/30/2015.
 */
public class RootTweetSolrRepository extends TweetsSolrRepository {
    public RootTweetSolrRepository(SolrServer solrServer) {
        super(solrServer);
    }
}
