package com.mongodb.balancer.strategy;

import com.mongodb.balancer.metrics.ShardMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Balancing strategy based on logical data size (before compression).
 * This is useful for balancing based on actual data volume.
 *
 * Higher scores = more data = priority to move chunks OFF
 * Lower scores = less data = priority to receive chunks
 */
public class DataSizeBalancingStrategy implements BalancingStrategy {

    private static final Logger logger = LoggerFactory.getLogger(DataSizeBalancingStrategy.class);

    private double weight;

    public DataSizeBalancingStrategy() {
        this.weight = 1.0;
    }

    public DataSizeBalancingStrategy(double weight) {
        this.weight = weight;
    }

    @Override
    public Map<String, Double> calculateShardScores(List<ShardMetrics> allShardMetrics) {
        Map<String, Double> scores = new HashMap<>();

        if (allShardMetrics.isEmpty()) {
            logger.warn("No shard metrics available for scoring");
            return scores;
        }

        // Find min and max data sizes for normalization
        long minData = Long.MAX_VALUE;
        long maxData = Long.MIN_VALUE;

        for (ShardMetrics metrics : allShardMetrics) {
            long dataSize = metrics.getDataSize();
            if (dataSize < minData) {
                minData = dataSize;
            }
            if (dataSize > maxData) {
                maxData = dataSize;
            }
        }

        // Calculate normalized scores (0.0 to 1.0)
        long range = maxData - minData;

        if (range == 0) {
            // All shards have the same data size, score them all as 0.5
            logger.info("All shards have equal data size, assigning neutral scores");
            for (ShardMetrics metrics : allShardMetrics) {
                scores.put(metrics.getShardId(), 0.5);
            }
        } else {
            for (ShardMetrics metrics : allShardMetrics) {
                long dataSize = metrics.getDataSize();
                double normalizedScore = (double) (dataSize - minData) / range;
                scores.put(metrics.getShardId(), normalizedScore);

                logger.debug("Shard {} - dataSize: {:.2f} GB, score: {:.3f}",
                           metrics.getShardId(), metrics.getDataSizeGB(), normalizedScore);
            }
        }

        logger.info("DataSize strategy scores: min={} GB (score=0.0), max={} GB (score=1.0)",
                   String.format("%.2f", minData / (1024.0 * 1024.0 * 1024.0)),
                   String.format("%.2f", maxData / (1024.0 * 1024.0 * 1024.0)));

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
        return "dataSize";
    }
}
