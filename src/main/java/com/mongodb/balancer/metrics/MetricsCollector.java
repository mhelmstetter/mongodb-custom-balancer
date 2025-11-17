package com.mongodb.balancer.metrics;

import java.util.List;

/**
 * Interface for collecting metrics from MongoDB shards.
 */
public interface MetricsCollector {

    /**
     * Collect current metrics from all shards in the cluster.
     *
     * @return list of shard metrics
     */
    List<ShardMetrics> collectShardMetrics();

    /**
     * Refresh/update the metrics from the cluster.
     * This should be called periodically to get fresh data.
     */
    void refreshMetrics();

    /**
     * Get the timestamp of when metrics were last collected.
     *
     * @return timestamp in milliseconds
     */
    long getLastCollectionTime();
}
