package com.neo4j.googlecloud.pubsub;

import org.junit.Before;
import org.junit.Rule;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.harness.junit.Neo4jRule;

public class AbstractPubsubTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withConfig("dbms.security.procedures.unrestricted","pubsub.*")
            .withConfig("google.pubsub.topic", "tmp")
            .withConfig("google.pubsub.project", "testbed-187316")
            .withFunction(com.neo4j.googlecloud.pubsub.userfn.UserFunctions.class);
    protected Driver driver;

    @Before
    public void setup() {
        this.driver = GraphDatabase.driver( neo4j.boltURI(), AuthTokens.basic( "neo4j", "neo4j"  ),
                Config.build().withoutEncryption().toConfig() );
    }
}
