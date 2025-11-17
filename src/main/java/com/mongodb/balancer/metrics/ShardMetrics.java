package com.mongodb.balancer.metrics;

import java.util.Objects;

/**
 * Metrics for a single shard, aggregated across all databases on that shard.
 */
public class ShardMetrics {

    private final String shardId;
    private final long storageSize;  // On-disk size in bytes (after compression)
    private final long dataSize;     // Logical data size in bytes (before compression)
    private final long indexSize;    // Index size in bytes
    private final int chunkCount;    // Number of chunks on this shard
    private final long timestamp;    // When metrics were collected

    public ShardMetrics(String shardId, long storageSize, long dataSize, long indexSize,
                        int chunkCount, long timestamp) {
        this.shardId = shardId;
        this.storageSize = storageSize;
        this.dataSize = dataSize;
        this.indexSize = indexSize;
        this.chunkCount = chunkCount;
        this.timestamp = timestamp;
    }

    public String getShardId() {
        return shardId;
    }

    public long getStorageSize() {
        return storageSize;
    }

    public long getDataSize() {
        return dataSize;
    }

    public long getIndexSize() {
        return indexSize;
    }

    public int getChunkCount() {
        return chunkCount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Get total size (storage + index) in bytes.
     */
    public long getTotalSize() {
        return storageSize + indexSize;
    }

    /**
     * Get storage size in GB for logging/display.
     */
    public double getStorageSizeGB() {
        return storageSize / (1024.0 * 1024.0 * 1024.0);
    }

    /**
     * Get data size in GB for logging/display.
     */
    public double getDataSizeGB() {
        return dataSize / (1024.0 * 1024.0 * 1024.0);
    }

    /**
     * Get total size in GB for logging/display.
     */
    public double getTotalSizeGB() {
        return getTotalSize() / (1024.0 * 1024.0 * 1024.0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardMetrics that = (ShardMetrics) o;
        return storageSize == that.storageSize &&
               dataSize == that.dataSize &&
               indexSize == that.indexSize &&
               chunkCount == that.chunkCount &&
               timestamp == that.timestamp &&
               Objects.equals(shardId, that.shardId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, storageSize, dataSize, indexSize, chunkCount, timestamp);
    }

    @Override
    public String toString() {
        return String.format("ShardMetrics{shardId='%s', storageSize=%.2fGB, dataSize=%.2fGB, " +
                           "indexSize=%.2fGB, chunkCount=%d, timestamp=%d}",
                           shardId, getStorageSizeGB(), getDataSizeGB(),
                           indexSize / (1024.0 * 1024.0 * 1024.0), chunkCount, timestamp);
    }
}
