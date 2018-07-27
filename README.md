Neo4j Streaming
===========

Neo4j server plugin for streaming node and edge messages to pub/sub
connectors.

1. Build it:

        mvn clean package

2. Copy target/pubsub*.jar to the plugins/ directory of your Neo4j server.

3.  Configure your Neo4j server:

```
pubsub.provider=google
pubsub.project=my-google-project-id
pubsub.topic=some-google-pubsub-topic-id
```

4. Start your Neo4j Server

4. Run any create query:

        CREATE (max:User {name:"Max"}) RETURN max;

5. You should see messages published to the Google Pubsub topic.
    

