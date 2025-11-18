package com.mongodb.balancer.metrics;

import com.mongodb.shardsync.ShardClient;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Collects shard metrics using MongoDB's dbStats command.
 * Aggregates statistics across all databases on each shard.
 */
public class DbStatsMetricsCollector implements MetricsCollector {

    private static final Logger logger = LoggerFactory.getLogger(DbStatsMetricsCollector.class);

    private final ShardClient shardClient;
    private List<ShardMetrics> cachedMetrics;
    private long lastCollectionTime;

    public DbStatsMetricsCollector(ShardClient shardClient) {
        this.shardClient = shardClient;
        this.cachedMetrics = new ArrayList<>();
        this.lastCollectionTime = 0;
    }

    @Override
    public List<ShardMetrics> collectShardMetrics() {
        refreshMetrics();
        return new ArrayList<>(cachedMetrics);
    }

    @Override
    public void refreshMetrics() {
        logger.info("Collecting shard metrics from cluster...");
        long startTime = System.currentTimeMillis();

        // Initialize metrics tracking for all known shards
        Map<String, ShardMetricsAccumulator> shardMetrics = new HashMap<>();
        for (String shardId : shardClient.getShardsMap().keySet()) {
            shardMetrics.put(shardId, new ShardMetricsAccumulator(shardId));
        }

        // Use mongos connection to get per-shard stats
        MongoClient mongosClient = shardClient.getMongoClient();

        // Get list of databases (excluding admin, config, local)
        List<String> databaseNames = new ArrayList<>();
        for (String dbName : mongosClient.listDatabaseNames()) {
            if (!dbName.equals("admin") && !dbName.equals("config") && !dbName.equals("local")) {
                databaseNames.add(dbName);
            }
        }

        logger.debug("Collecting stats for {} databases", databaseNames.size());

        // Collect per-shard stats for each database using dbStats through mongos
        for (String dbName : databaseNames) {
            try {
                MongoDatabase database = mongosClient.getDatabase(dbName);
                Document dbStats = database.runCommand(new Document("dbStats", 1).append("scale", 1));

                // For sharded clusters, dbStats returns a "raw" field with per-shard statistics
                Document rawDoc = dbStats.get("raw", Document.class);
                if (rawDoc != null) {
                    // Parse per-shard stats from the raw field
                    for (String shardKey : rawDoc.keySet()) {
                        Document shardStats = rawDoc.get(shardKey, Document.class);
                        if (shardStats != null) {
                            // Extract shard name from key like "shardName/host1:port,host2:port,..."
                            String shardId = shardKey.contains("/") ? shardKey.substring(0, shardKey.indexOf("/")) : shardKey;

                            ShardMetricsAccumulator accumulator = shardMetrics.get(shardId);
                            if (accumulator != null) {
                                accumulator.addDatabaseStats(dbName, shardStats);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to get stats for database {}: {}", dbName, e.getMessage());
            }
        }

        // Convert accumulators to ShardMetrics and add chunk counts
        List<ShardMetrics> newMetrics = new ArrayList<>();
        for (ShardMetricsAccumulator accumulator : shardMetrics.values()) {
            ShardMetrics metrics = accumulator.toShardMetrics(shardClient);
            newMetrics.add(metrics);
            logger.debug("Collected metrics for {}: {}", accumulator.shardId, metrics);
        }

        this.cachedMetrics = newMetrics;
        this.lastCollectionTime = System.currentTimeMillis();

        long elapsedMs = this.lastCollectionTime - startTime;
        logger.info("Metrics collection completed in {} ms for {} shards", elapsedMs, newMetrics.size());
    }

    /**
     * Helper class to accumulate metrics for a shard across multiple databases.
     */
    private static class ShardMetricsAccumulator {
        final String shardId;
        long totalStorageSize = 0;
        long totalDataSize = 0;
        long totalIndexSize = 0;

        ShardMetricsAccumulator(String shardId) {
            this.shardId = shardId;
        }

        void addDatabaseStats(String dbName, Document dbStats) {
            long storageSize = getNumberAsLong(dbStats, "storageSize");
            long dataSize = getNumberAsLong(dbStats, "dataSize");
            long indexSize = getNumberAsLong(dbStats, "indexSize");

            totalStorageSize += storageSize;
            totalDataSize += dataSize;
            totalIndexSize += indexSize;

            logger.debug("  {}/{}: storageSize={} GB, dataSize={} GB, indexSize={} GB",
                       shardId, dbName,
                       storageSize / (1024.0 * 1024.0 * 1024.0),
                       dataSize / (1024.0 * 1024.0 * 1024.0),
                       indexSize / (1024.0 * 1024.0 * 1024.0));
        }

        ShardMetrics toShardMetrics(ShardClient shardClient) {
            // Get chunk count for this shard from config.chunks (excluding config.system.sessions)
            int totalChunkCount = com.mongodb.util.ChunkUtils.getChunkCount(shardClient, shardId);
            logger.debug("  Total chunks on {} (excluding config.system.sessions): {}", shardId, totalChunkCount);

            return new ShardMetrics(
                shardId,
                totalStorageSize,
                totalDataSize,
                totalIndexSize,
                totalChunkCount,
                System.currentTimeMillis()
            );
        }
    }

    @Override
    public long getLastCollectionTime() {
        return lastCollectionTime;
    }

    /**
     * Helper method to extract a numeric value as Long from a Document.
     * MongoDB can return numbers as either Long or Double, so we need to handle both.
     */
    private static long getNumberAsLong(Document doc, String key) {
        Object value = doc.get(key);
        if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof Double) {
            return ((Double) value).longValue();
        } else if (value instanceof Integer) {
            return ((Integer) value).longValue();
        } else {
            throw new IllegalArgumentException(
                String.format("Expected numeric value for key '%s' but got %s", key,
                    value != null ? value.getClass().getSimpleName() : "null"));
        }
    }
}
