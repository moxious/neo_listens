package com.neo4j.streaming.pubsub;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PubSubConnector {
    protected Log log;
    protected ObjectMapper mapper;
    protected Publisher publisher;

    PubSubConnector(LogService logSvc) throws IOException {
        this.log = logSvc.getUserLog(PubSubConnector.class);
        this.mapper = new ObjectMapper();
        String topicId = PubsubConfiguration.get("topic", "UNDEFINED") + "";
        String projectId = PubsubConfiguration.get("project", "UNDEFINED") + "";
        String provider = PubsubConfiguration.get("provider", "UNDEFINED") + "";

        log.info("PubSub Connector: provider " + provider + " -> " + projectId + "/" + topicId);

        ProjectTopicName topic = ProjectTopicName.of(projectId, topicId);

        try {
            this.publisher = Publisher.newBuilder(topic).build();
        } catch(Exception exc) {
            log.error("Failed to create Google PubSub connector publisher:" + exc);
        }
    }

    private PubsubMessage toMsg(Map<String,Object> input) throws JsonProcessingException {
        byte [] json = mapper.writeValueAsBytes(input);
        ByteString data = ByteString.copyFrom(json);
        return PubsubMessage.newBuilder()
                .setData(data)
                .build();
    }

    private void sendMessage(Publisher pub, PubsubMessage msg) {
        log.info("Publishing message" + msg.getData().toString());
        final PubsubMessage m = msg;

        if (publisher != null) {
            ApiFuture<String> future = publisher.publish(m);
            ApiFutures.addCallback(future, new ApiFutureCallback<String>() {
                @Override
                public void onFailure(Throwable throwable) {
                    if (throwable instanceof ApiException) {
                        ApiException apiException = ((ApiException) throwable);
                        // details on the API exception
                        System.out.println(apiException.getStatusCode().getCode());
                        System.out.println(apiException.isRetryable());
                    }
                    log.error("Error publishing message : " + m);
                }

                @Override
                public void onSuccess(String messageId) {
                    // Once published, returns server-assigned message ids (unique within the topic)
                    log.info("Published " + messageId);
                }
            });
        } else {
            log.error("Message not published; no configured publisher.");
        }
    }

    public void send(Node n, Neo4jPubsubEventType et) throws JsonProcessingException {
        long id = n.getId();

//        List<String> labels = StreamSupport.stream(n.getLabels().spliterator(), false)
//                .map(Label::name)
//                .collect(Collectors.toList());

        List<String> labels = new ArrayList<String>();
        for(Label l : n.getLabels()) {
            labels.add(l.name());
        }

        HashMap<String,Object> data = new HashMap<>();
        data.put("entityType", "node");
        data.put("event", et.toString());
        data.put("id", id);
        data.put("labels", labels);
        data.put("properties", n.getAllProperties());

        sendMessage(publisher, toMsg(data));
    }

    public void send(Relationship r, Neo4jPubsubEventType et) throws JsonProcessingException {
        long id = r.getId();
        long start = r.getStartNodeId();
        long end = r.getEndNodeId();

        Map<String,Object> data = new HashMap<>();
        data.put("entityType", "relationship");
        data.put("event", et.toString());
        data.put("type", r.getType().name());
        data.put("id", id);
        data.put("start", start);
        data.put("end", end);
        data.put("properties", r.getAllProperties());

        sendMessage(publisher, toMsg(data));
    }
}
