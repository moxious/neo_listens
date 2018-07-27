package com.neo4j.streaming.pubsub;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

import java.io.IOException;

public class TransactionStreamer implements Runnable {
    private static TransactionData td;
    private static GraphDatabaseService db;
    private static Log log = null;
    private static PubSubConnector conn = null;
    private boolean enabled = true;

    public TransactionStreamer (TransactionData transactionData, GraphDatabaseService graphDatabaseService, LogService logsvc) {
        td = transactionData;
        db = graphDatabaseService;

        if (log == null) {
            log = logsvc.getUserLog(TransactionStreamer.class);
        }

        try {
            if (conn == null) {
                conn = new PubSubConnector(logsvc);
            }
        } catch (IOException exc) {
            log.error("Failed to initialize pubsub connector, disabling" + exc);
            enabled = false;
        }
    }

    @Override
    public void run() {
        if (!enabled) { return; }

        try (Transaction tx = db.beginTx()) {
           for (Node n : td.createdNodes()) {
                conn.send(n, Neo4jPubsubEventType.CREATE);
            }

            for (Relationship r : td.createdRelationships()) {
                conn.send(r, Neo4jPubsubEventType.CREATE);
            }

            for (Node n : td.deletedNodes()) {
                conn.send(n, Neo4jPubsubEventType.DELETE);
            }

            for (Relationship r : td.deletedRelationships()) {
                conn.send(r, Neo4jPubsubEventType.DELETE);
            }
        } catch(Exception e) {
            log.error("Exception processing transaction" + e);
        }
    }
}
