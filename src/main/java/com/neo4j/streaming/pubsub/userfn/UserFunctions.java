package com.neo4j.streaming.pubsub.userfn;

import com.neo4j.streaming.pubsub.PubSubConnector;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.io.IOException;
import java.util.Map;

public class UserFunctions {
    public static final String version = "0.0.1-SNAPSHOT";

    @UserFunction("pubsub.version")
    @Description("RETURN pubsub.version() | return the current pubsub installed version")
    public String version() {
        return version;
    }

    @UserFunction("pubsub.publish")
    @Description("RETURN pubsub.publish('google', 'my-project-id', 'pubsub-topic-id', { field1: 'value', field2: value })")
    public Map<String,Object> publish(
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
}
