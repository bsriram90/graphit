package edu.isu.coms.graphit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * Created by Naresh on 11/26/2015.
 */
@Component
public class ApplicationEnvironment {

    @Autowired
    private Environment environment;

    public String getMongodbUrl() {
        return this.environment.getProperty("mongodb.url");
    }

    public String getMongodbPassword() {
        return this.environment.getProperty("mongodb.password");
    }

    public String getMongodbDatabase() {
        return this.environment.getProperty("mongodb.database");
    }

    public String getMongodbUsername() {
        return this.environment.getProperty("mongodb.username");
    }


    public String getSolrUrlForTweets() {
        return this.environment.getProperty("solr.tweets.url");
    }

    public String getSolrUrlForRootTweets() {
        return this.environment.getProperty("solr.root_tweets.url");
    }

    public float getFacetAcceptanceThreshold() {
        return Float.parseFloat(this.environment.getProperty("facet.acceptance.threshold"));
    }
    public int getConversationsFilterLimit(){
        return Integer.parseInt(this.environment.getProperty("conversations.filter.limit"));
    }
}
