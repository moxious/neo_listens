package com.neo4j.streaming.pubsub.userfn;

import com.neo4j.streaming.pubsub.PubSubConnector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

public class UserFunctions {
    @Context public GraphDatabaseService db;
    @Context public Log log;

    public static final String version = "0.0.1-SNAPSHOT";

    @UserFunction("pubsub.version")
    @Description("RETURN pubsub.version() | return the current pubsub installed version")
    public String version() {
        return version;
    }

    @UserFunction("pubsub.publish.single")
    @Description("RETURN pubsub.publish.single('google', 'my-project-id', 'pubsub-topic-id', { field1: 'value', field2: value })")
    public Map<String,Object> publishSingle(
            @Name("provider") final String provider,
            @Name("project") final String project,
            @Name("topic") final String topic,
            @Name("message") final Map<String,Object> message) throws IOException {

        System.out.println("About to publish " + provider + "/" + project + "/" + topic + "/" + message);

        try {
            PubSubConnector connector = new PubSubConnector(provider, project, topic);
            return connector.sendMessage(message);
        } catch(Exception exc) {
            exc.printStackTrace();
            return null;
        }
    }

    @UserFunction("pubsub.publish.queryResult")
    @Description("RETURN pubsub.publish.queryResult('MATCH (p:Person { name: 'Emil' }) RETURN p'")
    public long publishQueryResult(
            @Name("query") final String query) throws IOException {
        long c = 0;

        PubSubConnector connector = new PubSubConnector();
        Result r = db.execute(query, Collections.emptyMap());

        while(r.hasNext()) {
            Map<String,Object> row = r.next();
            connector.sendMessage(row);
            c++;
        }

        r.close();
        return c;
    }
}
