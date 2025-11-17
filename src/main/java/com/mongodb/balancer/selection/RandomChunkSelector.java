package com.mongodb.balancer.selection;

import com.mongodb.balancer.metrics.ShardMetrics;
import com.mongodb.model.Megachunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * Selects a random chunk from the available chunks.
 * This provides unbiased selection and avoids always hitting the same chunks.
 */
public class RandomChunkSelector implements ChunkSelectionStrategy {

    private static final Logger logger = LoggerFactory.getLogger(RandomChunkSelector.class);
    private final Random random;

    public RandomChunkSelector() {
        this.random = new Random();
    }

    public RandomChunkSelector(long seed) {
        this.random = new Random(seed);
    }

    @Override
    public Megachunk selectChunk(ShardMetrics sourceShardMetrics, List<Megachunk> availableChunks) {
        if (availableChunks == null || availableChunks.isEmpty()) {
            logger.debug("No chunks available for selection on shard {}", sourceShardMetrics.getShardId());
            return null;
        }

        int randomIndex = random.nextInt(availableChunks.size());
        Megachunk selected = availableChunks.get(randomIndex);

        logger.debug("Randomly selected chunk {} from {} available chunks on shard {}",
                   randomIndex, availableChunks.size(), sourceShardMetrics.getShardId());

        return selected;
    }

    @Override
    public String getName() {
        return "random";
    }
}
