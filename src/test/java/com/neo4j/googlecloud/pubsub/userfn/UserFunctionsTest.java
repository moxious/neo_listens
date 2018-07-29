package com.neo4j.googlecloud.pubsub.userfn;

import com.neo4j.googlecloud.pubsub.AbstractPubsubTest;
import org.junit.Test;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;

import static org.junit.Assert.*;
import static org.neo4j.driver.v1.Values.parameters;

public class UserFunctionsTest extends AbstractPubsubTest {
    @Test
    public void providesVersion() {
        Session s = driver.session();
        String version = s.readTransaction(new TransactionWork<String>() {
            @Override
            public String execute(Transaction tx) {
                StatementResult result = tx.run("RETURN pubsub.version();",
                        parameters());
                return result.single().get(0).asString();
            }
        });
        s.close();

        assertEquals(version, UserFunctions.version);
    }
}