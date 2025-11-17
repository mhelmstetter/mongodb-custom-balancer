package com.mongodb.balancer.metrics;

import java.util.HashMap;
import java.util.Map;

/**
 * Tracks metrics for a single namespace (database.collection) across all shards.
 * Used to identify which namespaces are imbalanced and need active balancing.
 */
public class NamespaceMetrics {

    private final String namespace;
    private final Map<String, ShardNamespaceMetrics> perShardMetrics;
    private long timestamp;
    private boolean needsBalancing;
    private double imbalanceScore;  // Higher score = more imbalanced

    public NamespaceMetrics(String namespace) {
        this.namespace = namespace;
        this.perShardMetrics = new HashMap<>();
        this.timestamp = System.currentTimeMillis();
        this.needsBalancing = false;
        this.imbalanceScore = 0.0;
    }

    public void addShardMetrics(String shardId, long dataSize, long storageSize, long indexSize, int chunkCount) {
        perShardMetrics.put(shardId, new ShardNamespaceMetrics(shardId, dataSize, storageSize, indexSize, chunkCount));
    }

    /**
     * Calculate imbalance score based on data size distribution.
     * Returns the ratio of (max - min) / average data size across shards.
     *
     * @param threshold Imbalance score threshold (e.g., 0.1 = 10% variation)
     */
    public void calculateImbalanceScore(double threshold) {
        if (perShardMetrics.isEmpty()) {
            this.imbalanceScore = 0.0;
            this.needsBalancing = false;
            return;
        }

        long minDataSize = Long.MAX_VALUE;
        long maxDataSize = 0;
        long totalDataSize = 0;
        int shardCount = 0;

        for (ShardNamespaceMetrics metrics : perShardMetrics.values()) {
            long dataSize = metrics.getDataSize();
            if (dataSize > 0) {  // Only count shards that have data for this namespace
                minDataSize = Math.min(minDataSize, dataSize);
                maxDataSize = Math.max(maxDataSize, dataSize);
                totalDataSize += dataSize;
                shardCount++;
            }
        }

        if (shardCount == 0 || totalDataSize == 0) {
            this.imbalanceScore = 0.0;
            this.needsBalancing = false;
            return;
        }

        double avgDataSize = (double) totalDataSize / shardCount;
        long difference = maxDataSize - minDataSize;

        // Imbalance score: ratio of difference to average
        // e.g., if max=1000GB, min=100GB, avg=550GB, score = 900/550 = 1.64
        this.imbalanceScore = avgDataSize > 0 ? (double) difference / avgDataSize : 0.0;

        // Consider it imbalanced if the difference exceeds the threshold
        this.needsBalancing = this.imbalanceScore > threshold;
    }

    public String getNamespace() {
        return namespace;
    }

    public Map<String, ShardNamespaceMetrics> getPerShardMetrics() {
        return perShardMetrics;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean needsBalancing() {
        return needsBalancing;
    }

    public double getImbalanceScore() {
        return imbalanceScore;
    }

    public long getTotalDataSize() {
        return perShardMetrics.values().stream()
            .mapToLong(ShardNamespaceMetrics::getDataSize)
            .sum();
    }

    public int getTotalChunkCount() {
        return perShardMetrics.values().stream()
            .mapToInt(ShardNamespaceMetrics::getChunkCount)
            .sum();
    }

    @Override
    public String toString() {
        return String.format("NamespaceMetrics{namespace='%s', shards=%d, totalData=%d GB, imbalance=%.2f, needsBalancing=%s}",
            namespace, perShardMetrics.size(), getTotalDataSize() / (1024L * 1024 * 1024), imbalanceScore, needsBalancing);
    }

    /**
     * Metrics for a single namespace on a single shard.
     */
    public static class ShardNamespaceMetrics {
        private final String shardId;
        private final long dataSize;
        private final long storageSize;
        private final long indexSize;
        private final int chunkCount;

        public ShardNamespaceMetrics(String shardId, long dataSize, long storageSize, long indexSize, int chunkCount) {
            this.shardId = shardId;
            this.dataSize = dataSize;
            this.storageSize = storageSize;
            this.indexSize = indexSize;
            this.chunkCount = chunkCount;
        }

        public String getShardId() {
            return shardId;
        }

        public long getDataSize() {
            return dataSize;
        }

        public long getStorageSize() {
            return storageSize;
        }

        public long getIndexSize() {
            return indexSize;
        }

        public int getChunkCount() {
            return chunkCount;
        }
    }
}
