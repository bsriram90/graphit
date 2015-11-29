package edu.isu.coms.graphit.repositories;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
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

    public List<String> getHashtagsForRootNodes(String[] keywords, float acceptancePercentage){
        SolrQuery query = new SolrQuery();
        List<String> results = new ArrayList<String>();
        String textQuery = "";
        for (String keyword : keywords) {
            if (!textQuery.equals("")) {
                textQuery += " OR ";
            }
            textQuery += keyword;
        }
        textQuery = "(" + textQuery + ")";
        query.set("q", "text:" + textQuery);
        query.setFacet(true);
        query.setRows(0);
        query.addFacetField("hashtags");
        try {
            QueryResponse response = this.solrServer.query(query);
            // acceptance threshold is 1%
            float acceptanceThreshold = response.getResults().getNumFound() * (acceptancePercentage / 100);
            System.out.println("Total docs = " + response.getResults().getNumFound());
            System.out.println("Acceptance threshold = " + acceptanceThreshold);
            List<FacetField> facetFields = response.getFacetFields();
            for(FacetField facetField : facetFields) {
                if(facetField.getName().equals("hashtags")) {
                    List<FacetField.Count> counts = facetField.getValues();
                    for(FacetField.Count count : counts) {
                        if(count.getCount() >= acceptanceThreshold) {
                            results.add(count.getName());
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }
}

