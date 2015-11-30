package edu.isu.coms.graphit.repositories;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
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
        query.set("q", "text:" + getQueryText(keywords));
        query.setFacet(true);
        query.setRows(0);
        query.addFacetField("hashtags");
        try {
            QueryResponse response = this.solrServer.query(query);
            float acceptanceThreshold = response.getResults().getNumFound() * (acceptancePercentage / 100);
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

    private String getQueryText(Object[] keywords) {
        String textQuery = "";
        for (Object keyword : keywords) {
            if (!textQuery.equals("")) {
                textQuery += " OR ";
            }
            textQuery += (String)keyword;
        }
        textQuery = "(" + textQuery + ")";
        return textQuery;
    }

    public void findRootTweets(List<String> hashtags, float acceptancePercentage) {
        SolrQuery query = new SolrQuery();
        String queryString = getQueryText(hashtags.toArray());
        query.set("q", "text:" +queryString + ", hashtags:" + queryString);
        try {
            QueryResponse response = this.solrServer.query(query);
            SolrDocumentList results = response.getResults();
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
    }
}

