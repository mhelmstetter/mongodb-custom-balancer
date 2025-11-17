package com.mongodb.balancer.strategy;

import com.mongodb.balancer.metrics.ShardMetrics;
import java.util.List;
import java.util.Map;

/**
 * Interface for balancing strategies that score shards based on various metrics.
 * Higher scores indicate shards that are more overloaded and should have chunks moved OFF.
 * Lower scores indicate shards that are underutilized and should receive chunks.
 */
public interface BalancingStrategy {

    /**
     * Calculate scores for all shards based on this strategy's criteria.
     * Scores are normalized between 0.0 and 1.0, where:
     * - 1.0 = most overloaded (highest priority to move chunks OFF this shard)
     * - 0.0 = least loaded (highest priority to move chunks TO this shard)
     *
     * @param allShardMetrics metrics for all shards in the cluster
     * @return map of shard ID to score (0.0 to 1.0)
     */
    Map<String, Double> calculateShardScores(List<ShardMetrics> allShardMetrics);

    /**
     * Get the weight of this strategy when combining with other strategies.
     * Higher weights give this strategy more influence in the final decision.
     *
     * @return the weight (typically between 0.0 and 10.0)
     */
    double getWeight();

    /**
     * Set the weight for this strategy.
     *
     * @param weight the weight to set
     */
    void setWeight(double weight);

    /**
     * Get the name of this strategy for logging and configuration.
     *
     * @return strategy name (e.g., "diskSpace", "dataSize", "chunkCount")
     */
    String getName();
}
