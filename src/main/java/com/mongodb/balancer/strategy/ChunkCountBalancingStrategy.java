package com.mongodb.balancer.strategy;

import com.mongodb.balancer.metrics.ShardMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Balancing strategy based on number of chunks per shard.
 * This is useful for evening out chunk distribution.
 *
 * Higher scores = more chunks = priority to move chunks OFF
 * Lower scores = fewer chunks = priority to receive chunks
 */
public class ChunkCountBalancingStrategy implements BalancingStrategy {

    private static final Logger logger = LoggerFactory.getLogger(ChunkCountBalancingStrategy.class);

    private double weight;

    public ChunkCountBalancingStrategy() {
        this.weight = 1.0;
    }

    public ChunkCountBalancingStrategy(double weight) {
        this.weight = weight;
    }

    @Override
    public Map<String, Double> calculateShardScores(List<ShardMetrics> allShardMetrics) {
        Map<String, Double> scores = new HashMap<>();

        if (allShardMetrics.isEmpty()) {
            logger.warn("No shard metrics available for scoring");
            return scores;
        }

        // Find min and max chunk counts for normalization
        int minChunks = Integer.MAX_VALUE;
        int maxChunks = Integer.MIN_VALUE;

        for (ShardMetrics metrics : allShardMetrics) {
            int chunkCount = metrics.getChunkCount();
            if (chunkCount < minChunks) {
                minChunks = chunkCount;
            }
            if (chunkCount > maxChunks) {
                maxChunks = chunkCount;
            }
        }

        // Calculate normalized scores (0.0 to 1.0)
        int range = maxChunks - minChunks;

        if (range == 0) {
            // All shards have the same chunk count, score them all as 0.5
            logger.info("All shards have equal chunk counts, assigning neutral scores");
            for (ShardMetrics metrics : allShardMetrics) {
                scores.put(metrics.getShardId(), 0.5);
            }
        } else {
            for (ShardMetrics metrics : allShardMetrics) {
                int chunkCount = metrics.getChunkCount();
                double normalizedScore = (double) (chunkCount - minChunks) / range;
                scores.put(metrics.getShardId(), normalizedScore);

                logger.debug("Shard {} - chunks: {}, score: {}",
                           metrics.getShardId(), chunkCount, normalizedScore);
            }
        }

        logger.info("ChunkCount strategy scores: min={} chunks (score=0.0), max={} chunks (score=1.0)",
                   minChunks, maxChunks);

        return scores;
    }

    @Override
    public double getWeight() {
        return weight;
    }

    @Override
    public void setWeight(double weight) {
        this.weight = weight;
    }

    @Override
    public String getName() {
        return "chunkCount";
    }
}
