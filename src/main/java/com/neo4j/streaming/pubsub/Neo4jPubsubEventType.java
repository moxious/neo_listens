package com.neo4j.streaming.pubsub;

public enum Neo4jPubsubEventType {
    CREATE {
        public String toString() { return "create"; }
    },

    DELETE {
        public String toString() { return "delete"; }
    }
}
