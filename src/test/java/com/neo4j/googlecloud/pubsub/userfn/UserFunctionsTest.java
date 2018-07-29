package com.neo4j.googlecloud.pubsub.userfn;

import com.neo4j.googlecloud.pubsub.AbstractPubsubTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;

import static org.junit.Assert.*;
import static org.neo4j.driver.v1.Values.parameters;

public class UserFunctionsTest extends AbstractPubsubTest {
    public Session s;
    private static String project = "testbed-187316";
    private static String topic = "tmp";

    @Before
    public void sessionSetup() {
        this.s = driver.session();
    }

    @After
    public void sessionTeardown() {
        s.close();
    }

    @Test
    public void providesVersion() {
        String version = s.readTransaction(new TransactionWork<String>() {
            @Override
            public String execute(Transaction tx) {
                StatementResult result = tx.run("RETURN google.pubsub.version();",
                        parameters());
                return result.single().get(0).asString();
            }
        });

        assertEquals(version, UserFunctions.version);
    }

    @Test
    public void publishMessage() throws InterruptedException {
        Object result = s.readTransaction(new TransactionWork<Object>() {
            String q = "RETURN google.pubsub.publish.message('" + project + "', '" + topic + "', " +
                    "{foo:'bar', x: 1});";
            public Object execute(Transaction tx) {
                StatementResult r = tx.run(q);
                return r.single().get(0);
            }
        });

        Thread.sleep(4000);
        System.out.println(result);
    }
}