package edu.isu.coms.graphit.repositories;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.List;

/**
 * Created by Naresh on 11/27/2015.
 */
@Repository
public class SolrRepository {
    private SolrServer solrServer;

    @Autowired
    public SolrRepository(SolrServer solrServer) {
        this.solrServer = solrServer;
    }


    public void index(SolrInputDocument solrDoc) throws IOException, SolrServerException {
        solrServer.add(solrDoc);
        solrServer.commit();
    }
    public void index(List<SolrInputDocument> solrDocs) throws IOException, SolrServerException {
        solrServer.add(solrDocs);
        solrServer.commit();
    }
}

