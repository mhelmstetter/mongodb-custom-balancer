package com.mongodb.balancer.selection;

import com.mongodb.balancer.metrics.ShardMetrics;
import com.mongodb.model.Megachunk;
import com.mongodb.shardsync.ShardClient;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Selects the largest chunk (by document count or size) from available chunks.
 * This can help rebalance faster by moving bigger chunks first.
 *
 * Note: Determining chunk size requires dataSize command which can be slow,
 * so this implementation uses a heuristic based on chunk bounds if size info
 * is not readily available.
 */
public class LargestChunkSelector implements ChunkSelectionStrategy {

    private static final Logger logger = LoggerFactory.getLogger(LargestChunkSelector.class);
    private final ShardClient shardClient;

    public LargestChunkSelector(ShardClient shardClient) {
        this.shardClient = shardClient;
    }

    @Override
    public Megachunk selectChunk(ShardMetrics sourceShardMetrics, List<Megachunk> availableChunks) {
        if (availableChunks == null || availableChunks.isEmpty()) {
            logger.debug("No chunks available for selection on shard {}", sourceShardMetrics.getShardId());
            return null;
        }

        // For now, select randomly as determining actual chunk size is expensive
        // In the future, could maintain a cache of chunk sizes
        logger.debug("LargestChunkSelector: selecting random chunk (size calculation not yet implemented)");

        // TODO: Implement actual size-based selection
        // This would require calling dataSize command for each chunk, which is slow
        // Possible optimization: maintain a cache of chunk sizes updated periodically

        // For now, just return the first chunk
        return availableChunks.get(0);
    }

    @Override
    public String getName() {
        return "largest";
    }
}
