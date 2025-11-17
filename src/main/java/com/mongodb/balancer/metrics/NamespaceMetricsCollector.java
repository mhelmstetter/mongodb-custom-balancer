package com.mongodb.balancer.metrics;

import com.mongodb.shardsync.ShardClient;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Collects per-namespace metrics to identify which namespaces are imbalanced.
 * Uses collStats for targeted collection instead of scanning all databases.
 */
public class NamespaceMetricsCollector {

    private static final Logger logger = LoggerFactory.getLogger(NamespaceMetricsCollector.class);

    private final ShardClient shardClient;
    private final Map<String, NamespaceMetrics> namespaceMetricsCache;
    private long lastFullScanTime;
    private double imbalanceThreshold;

    public NamespaceMetricsCollector(ShardClient shardClient, double imbalanceThreshold) {
        this.shardClient = shardClient;
        this.namespaceMetricsCache = new HashMap<>();
        this.lastFullScanTime = 0;
        this.imbalanceThreshold = imbalanceThreshold;
    }

    /**
     * Scan all namespaces in the cluster and identify which ones are imbalanced.
     * This is expensive and should be called infrequently (e.g., on startup, every 30 minutes).
     *
     * @return List of namespaces that need balancing, sorted by imbalance score (most imbalanced first)
     */
    public List<String> scanAllNamespaces() {
        logger.info("Performing full namespace scan to identify imbalances...");
        long startTime = System.currentTimeMillis();

        MongoClient mongosClient = shardClient.getMongoClient();
        Set<String> allShardIds = shardClient.getShardsMap().keySet();

        // Get list of all sharded collections from config.collections
        List<String> shardedNamespaces = getShardedCollections();
        logger.info("Found {} sharded collections to analyze", shardedNamespaces.size());

        // Collect metrics for each namespace
        for (String namespace : shardedNamespaces) {
            try {
                NamespaceMetrics nsMetrics = collectNamespaceMetrics(namespace, allShardIds);
                if (nsMetrics != null) {
                    namespaceMetricsCache.put(namespace, nsMetrics);
                }
            } catch (Exception e) {
                logger.warn("Failed to collect metrics for namespace {}: {}", namespace, e.getMessage());
            }
        }

        lastFullScanTime = System.currentTimeMillis();
        long duration = lastFullScanTime - startTime;

        // Sort all namespaces by imbalance score for debugging
        List<NamespaceMetrics> sortedMetrics = namespaceMetricsCache.values().stream()
            .sorted((a, b) -> Double.compare(b.getImbalanceScore(), a.getImbalanceScore()))
            .collect(Collectors.toList());

        // Log top 20 to see what the scores actually are
        logger.info("Top 20 namespaces by imbalance score:");
        for (int i = 0; i < Math.min(20, sortedMetrics.size()); i++) {
            NamespaceMetrics metrics = sortedMetrics.get(i);
            logger.info("  #{}: {} - score={}, totalData={} GB, totalChunks={}, needsBalance={}",
                i + 1, metrics.getNamespace(),
                String.format("%.3f", metrics.getImbalanceScore()),
                String.format("%.2f", metrics.getTotalDataSize() / (1024.0 * 1024 * 1024)),
                metrics.getTotalChunkCount(),
                metrics.needsBalancing());
        }

        // Filter namespaces that need balancing
        List<String> imbalancedNamespaces = sortedMetrics.stream()
            .filter(NamespaceMetrics::needsBalancing)
            .map(NamespaceMetrics::getNamespace)
            .collect(Collectors.toList());

        logger.info("Full namespace scan completed in {} ms. Found {} imbalanced namespaces out of {} total",
            duration, imbalancedNamespaces.size(), shardedNamespaces.size());

        return imbalancedNamespaces;
    }

    /**
     * Refresh metrics for specific namespaces only (much faster than full scan).
     *
     * @param namespaces List of namespaces to refresh
     */
    public void refreshNamespaces(List<String> namespaces) {
        if (namespaces == null || namespaces.isEmpty()) {
            return;
        }

        logger.debug("Refreshing metrics for {} namespaces", namespaces.size());
        long startTime = System.currentTimeMillis();

        Set<String> allShardIds = shardClient.getShardsMap().keySet();

        for (String namespace : namespaces) {
            try {
                NamespaceMetrics nsMetrics = collectNamespaceMetrics(namespace, allShardIds);
                if (nsMetrics != null) {
                    namespaceMetricsCache.put(namespace, nsMetrics);
                }
            } catch (Exception e) {
                logger.warn("Failed to refresh metrics for namespace {}: {}", namespace, e.getMessage());
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        logger.debug("Refreshed {} namespaces in {} ms", namespaces.size(), duration);
    }

    /**
     * Collect metrics for a single namespace across all shards.
     */
    private NamespaceMetrics collectNamespaceMetrics(String namespace, Set<String> shardIds) {
        String[] parts = namespace.split("\\.", 2);
        if (parts.length != 2) {
            logger.warn("Invalid namespace format: {}", namespace);
            return null;
        }

        String dbName = parts[0];
        String collName = parts[1];

        NamespaceMetrics nsMetrics = new NamespaceMetrics(namespace);

        // Get collStats with scale=1 (bytes) through mongos
        // The "raw" field will contain per-shard statistics
        try {
            MongoClient mongosClient = shardClient.getMongoClient();
            MongoDatabase database = mongosClient.getDatabase(dbName);
            Document collStats = database.runCommand(new Document("collStats", collName).append("scale", 1));

            // Parse per-shard stats from the raw field
            Document rawDoc = collStats.get("raw", Document.class);
            if (rawDoc != null) {
                for (String shardKey : rawDoc.keySet()) {
                    Document shardStats = rawDoc.get(shardKey, Document.class);
                    if (shardStats != null) {
                        // Extract shard name from key like "shardName/host1:port,host2:port,..."
                        String shardId = shardKey.contains("/") ? shardKey.substring(0, shardKey.indexOf("/")) : shardKey;

                        long storageSize = getNumberAsLong(shardStats, "storageSize", 0L);
                        long dataSize = getNumberAsLong(shardStats, "size", 0L);  // "size" is the uncompressed data size
                        long indexSize = getNumberAsLong(shardStats, "totalIndexSize", 0L);
                        int chunkCount = shardStats.getInteger("nchunks", 0);

                        nsMetrics.addShardMetrics(shardId, dataSize, storageSize, indexSize, chunkCount);
                    }
                }
            }

            // Calculate imbalance score with configured threshold
            nsMetrics.calculateImbalanceScore(imbalanceThreshold);

        } catch (Exception e) {
            logger.warn("Failed to get collStats for {}: {}", namespace, e.getMessage());
            return null;
        }

        return nsMetrics;
    }

    /**
     * Get list of all sharded collections from config.collections.
     */
    private List<String> getShardedCollections() {
        List<String> namespaces = new ArrayList<>();

        try {
            MongoClient mongosClient = shardClient.getMongoClient();
            MongoDatabase configDb = mongosClient.getDatabase("config");

            // Query config.collections for all sharded collections
            for (Document doc : configDb.getCollection("collections").find()) {
                String ns = doc.getString("_id");
                if (ns != null && !ns.startsWith("config.")) {
                    namespaces.add(ns);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to query config.collections: {}", e.getMessage());
        }

        return namespaces;
    }

    /**
     * Get cached namespace metrics.
     */
    public NamespaceMetrics getNamespaceMetrics(String namespace) {
        return namespaceMetricsCache.get(namespace);
    }

    /**
     * Get all cached namespace metrics.
     */
    public Map<String, NamespaceMetrics> getAllNamespaceMetrics() {
        return new HashMap<>(namespaceMetricsCache);
    }

    public long getLastFullScanTime() {
        return lastFullScanTime;
    }

    /**
     * Generate shard-level metrics by aggregating namespace metrics.
     * This is much faster than running dbStats when namespace-focus is enabled.
     *
     * @return List of ShardMetrics aggregated from active namespaces
     */
    public List<com.mongodb.balancer.metrics.ShardMetrics> generateShardMetrics() {
        Map<String, ShardAggregator> shardAggregators = new HashMap<>();

        // Initialize aggregators for all known shards
        for (String shardId : shardClient.getShardsMap().keySet()) {
            shardAggregators.put(shardId, new ShardAggregator(shardId));
        }

        // Aggregate data from all namespace metrics
        for (NamespaceMetrics nsMetrics : namespaceMetricsCache.values()) {
            for (Map.Entry<String, NamespaceMetrics.ShardNamespaceMetrics> entry :
                    nsMetrics.getPerShardMetrics().entrySet()) {
                String shardId = entry.getKey();
                NamespaceMetrics.ShardNamespaceMetrics shardNsMetrics = entry.getValue();

                ShardAggregator aggregator = shardAggregators.get(shardId);
                if (aggregator != null) {
                    aggregator.add(shardNsMetrics.getDataSize(),
                                   shardNsMetrics.getStorageSize(),
                                   shardNsMetrics.getIndexSize(),
                                   shardNsMetrics.getChunkCount());
                }
            }
        }

        // Convert to ShardMetrics list
        List<com.mongodb.balancer.metrics.ShardMetrics> shardMetrics = new ArrayList<>();
        for (ShardAggregator aggregator : shardAggregators.values()) {
            shardMetrics.add(aggregator.toShardMetrics());
        }

        return shardMetrics;
    }

    /**
     * Helper class to aggregate metrics for a shard across multiple namespaces.
     */
    private static class ShardAggregator {
        final String shardId;
        long totalDataSize = 0;
        long totalStorageSize = 0;
        long totalIndexSize = 0;
        int totalChunkCount = 0;

        ShardAggregator(String shardId) {
            this.shardId = shardId;
        }

        void add(long dataSize, long storageSize, long indexSize, int chunkCount) {
            this.totalDataSize += dataSize;
            this.totalStorageSize += storageSize;
            this.totalIndexSize += indexSize;
            this.totalChunkCount += chunkCount;
        }

        com.mongodb.balancer.metrics.ShardMetrics toShardMetrics() {
            return new com.mongodb.balancer.metrics.ShardMetrics(
                shardId,
                totalStorageSize,
                totalDataSize,
                totalIndexSize,
                totalChunkCount,
                System.currentTimeMillis()
            );
        }
    }

    /**
     * Helper method to extract a numeric value as Long from a Document.
     * Returns default value if key doesn't exist or value is null.
     */
    private static long getNumberAsLong(Document doc, String key, long defaultValue) {
        Object value = doc.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof Double) {
            return ((Double) value).longValue();
        } else if (value instanceof Integer) {
            return ((Integer) value).longValue();
        } else {
            logger.warn("Unexpected type for key '{}': {}, using default value", key, value.getClass().getSimpleName());
            return defaultValue;
        }
    }
}
