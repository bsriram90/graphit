package edu.isu.coms.graphit.config;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import edu.isu.coms.graphit.ApplicationEnvironment;
import edu.isu.coms.graphit.repositories.SolrRepository;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.MalformedURLException;

/**
 * Created by Naresh on 11/26/2015.
 */
@Configuration
public class SpringConfig {

    @Autowired
    private ApplicationEnvironment applicationEnvironment;

    @Bean(name = "tweets")
    public DBCollection tweets() throws Exception {
        return mongoApplicationDB().getCollection("tweets");
    }

    @Bean(name = "tweetsdump")
    public DBCollection tweetsdump() throws Exception {
        return mongoApplicationDB().getCollection("tweetsdump");
    }

    @Bean(name = "searchmetadata")
    public DBCollection searchMetadata() throws Exception {
        return mongoApplicationDB().getCollection("searchMetadata");
    }

    @Bean(name = "roottweets")
    public DBCollection rootTeets() throws Exception {
        return mongoApplicationDB().getCollection("root_tweets");
    }

    @Bean(name = "conversations")
    public DBCollection conversations() throws Exception {
        return mongoApplicationDB().getCollection("conversations");
    }

    @Bean(name = "stats")
    public DBCollection stats() throws Exception {
        return mongoApplicationDB().getCollection("stats");
    }

    @Bean
    public MongoClient mongo() throws Exception {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
          return new MongoClient(applicationEnvironment.getMongodbUrl(), builder.build());
    }
    @Bean(name = "applicationDB")
    public DB mongoApplicationDB() throws Exception {
        return authenticate(applicationEnvironment.getMongodbDatabase(), applicationEnvironment.getMongodbUsername(), applicationEnvironment.getMongodbPassword());
    }

    private DB authenticate(String database, String username, String password) throws Exception {
        DB db = mongo().getDB(database);
        if (username != null && !db.isAuthenticated()) {
            db.authenticate(username, (password != null) ? password.toCharArray() : null);
        }
        return db;
    }

    @Bean
    public SolrRepository solrRepository() throws MalformedURLException {
        return new SolrRepository(solrServer());
    }

    @Bean
    public SolrServer solrServer() throws MalformedURLException {
        HttpSolrServer commonsHttpSolrServer = new HttpSolrServer(applicationEnvironment.getSolrUrl());
        commonsHttpSolrServer.setParser(new XMLResponseParser());
        return commonsHttpSolrServer;
    }
}
