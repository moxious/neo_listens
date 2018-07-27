package com.neo4j.streaming.pubsub;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.neo4j.streaming.pubsub.serializers.DateTimeValueSerializer;
import com.neo4j.streaming.pubsub.serializers.DateValueSerializer;
import com.neo4j.streaming.pubsub.serializers.PointValueSerializer;
import com.neo4j.streaming.pubsub.serializers.TemporalValueSerializer;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TemporalValue;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class PubSubConnector {
    private static Log log;
    private static ObjectMapper mapper;
    private static SimpleModule module;
    private static DateFormat df;

    private ProjectTopicName topic;
    private Publisher publisher;

    public static void initialize(LogService logSvc) {
        if (log == null)
            log = logSvc.getUserLog(PubSubConnector.class);

        mapper = new ObjectMapper();

        module = new SimpleModule();
        module.addSerializer(TemporalValue.class, new TemporalValueSerializer());
        module.addSerializer(PointValue.class, new PointValueSerializer());
        module.addSerializer(DateTimeValue.class, new DateTimeValueSerializer());
        module.addSerializer(DateValue.class, new DateValueSerializer());

        mapper.registerModule(module);

        TimeZone tz = TimeZone.getTimeZone("UTC");
        df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        df.setTimeZone(tz);
    }

    public PubSubConnector() throws IOException {
        String topicId = PubsubConfiguration.get("topic", "UNDEFINED") + "";
        String projectId = PubsubConfiguration.get("project", "UNDEFINED") + "";
        String provider = PubsubConfiguration.get("provider", "UNDEFINED") + "";

        this.topic = ProjectTopicName.of(projectId, topicId);

        log.info("PubSub Connector: provider " + provider + " -> " + this.describe());

        try {
            this.publisher = Publisher.newBuilder(topic).build();
        } catch(Exception exc) {
            log.error("Failed to create " + provider + " PubSub connector publisher:" + exc);
        }
    }

    public PubSubConnector(String provider, String projectId, String topicId) throws IOException {
        this.topic = ProjectTopicName.of(projectId, topicId);

        log.info("PubSub Connector: provider " + provider + " -> " + this.describe());

        try {
            this.publisher = Publisher.newBuilder(topic).build();
        } catch(Exception exc) {
            log.error("Failed to create " + provider + " PubSub connector publisher: " + exc);
        }
    }

    public String describe() {
        return this.topic.getProject() + "/" + this.topic.getTopic();
    }

    private PubsubMessage toMsg(Map<String,Object> input) throws JsonProcessingException {
        String nowAsISO = df.format(new Date());
        input.put("time", nowAsISO);
        byte [] json = mapper.writeValueAsBytes(input);
        ByteString data = ByteString.copyFrom(json);
        return PubsubMessage.newBuilder()
                .setData(data)
                .build();
    }

    public Map<String,Object> sendMessage(Map<String,Object> serializableMessageParams) throws JsonProcessingException {
        return sendMessage(toMsg(serializableMessageParams));
    }

    private Map<String,Object> sendMessage(PubsubMessage msg) {
        log.info("Publishing message" + msg.getData().toString());
        final PubsubMessage m = msg;

        if (publisher != null) {
            ApiFuture<String> future = publisher.publish(m);

            ApiFutures.addCallback(future, new ApiFutureCallback<String>() {
                public void onFailure(Throwable throwable) {
                    if (throwable instanceof ApiException) {
                        ApiException apiException = ((ApiException) throwable);
                        // details on the API exception
                        System.out.println(apiException.getStatusCode().getCode());
                        System.out.println(apiException.isRetryable());
                    }
                    throwable.printStackTrace();
                    log.error("Error publishing message " + m, throwable);
                }

                public void onSuccess(String messageId) {
                    // Once published, returns server-assigned message ids (unique within the topic)
//                    log.info("Published " + messageId);
                }
            });

            Map<String,Object> map = new HashMap<>();
            map.put("cancelled", future.isCancelled());
            map.put("done", future.isDone());
            return map;
        } else {
            log.error("Message not published; no configured publisher.");
            return null;
        }
    }

    public Map<String,Object> send(Node n, Neo4jPubsubEventType et) throws JsonProcessingException {
        List<String> labels = new ArrayList<>();
        for(Label l : n.getLabels()) {
            labels.add(l.name());
        }

        HashMap<String,Object> data = new HashMap<>();
        data.put("entityType", "node");
        data.put("event", et.toString());
        data.put("id", n.getId());
        data.put("labels", labels);
        data.put("properties", n.getAllProperties());

        return sendMessage(data);
    }

    public Map<String,Object> send(Relationship r, Neo4jPubsubEventType et) throws JsonProcessingException {
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

        return sendMessage(data);
    }
}
