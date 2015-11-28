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


    public String getSolrUrl() {
        return this.environment.getProperty("solr.url");
    }
}
