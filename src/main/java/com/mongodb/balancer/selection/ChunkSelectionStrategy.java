package com.mongodb.balancer.selection;

import com.mongodb.balancer.metrics.ShardMetrics;
import com.mongodb.model.Megachunk;
import java.util.List;

/**
 * Interface for strategies that select which chunk to move from a shard.
 */
public interface ChunkSelectionStrategy {

    /**
     * Select a chunk to move from the given shard.
     *
     * @param sourceShardMetrics metrics for the source shard
     * @param availableChunks list of chunks available to move from this shard
     * @return the selected chunk, or null if no suitable chunk found
     */
    Megachunk selectChunk(ShardMetrics sourceShardMetrics, List<Megachunk> availableChunks);

    /**
     * Get the name of this selection strategy.
     *
     * @return strategy name (e.g., "random", "largest", "smallest")
     */
    String getName();
}
