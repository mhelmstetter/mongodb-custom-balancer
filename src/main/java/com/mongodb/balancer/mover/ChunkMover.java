package com.mongodb.balancer.mover;

import java.util.HashSet;
import java.util.Set;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoCommandException;
import com.mongodb.model.Megachunk;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.shardsync.MoveChunkResult;

/**
 * Handles chunk movement with automatic version detection.
 * Uses moveRange for MongoDB 6.0+ and moveChunk for older versions.
 * Version detection is now handled by ShardClient.
 */
public class ChunkMover {

    private static final Logger logger = LoggerFactory.getLogger(ChunkMover.class);

    private static final int CHUNK_TOO_BIG_ERROR_CODE = 13403;
    private static final int MINIMUM_MOVE_INTERVAL_MINUTES = 5;

    private final ShardClient shardClient;
    private final boolean useSecondaryThrottle;
    private final boolean waitForDelete;
    private final boolean dryRun;
    private final Set<String> movedChunks;  // Track moved chunks to avoid thrashing
    private boolean useMoveRange;  // Use moveRange command for MongoDB 6.0+

    public ChunkMover(ShardClient shardClient, boolean useSecondaryThrottle,
                     boolean waitForDelete, boolean dryRun) {
        this.shardClient = shardClient;
        this.useSecondaryThrottle = useSecondaryThrottle;
        this.waitForDelete = waitForDelete;
        this.dryRun = dryRun;
        this.movedChunks = new HashSet<>();
        this.useMoveRange = false;  // Will be set in detectMongoVersion()
    }

    /**
     * Detect MongoDB version and determine which move command to use.
     * Should be called during initialization.
     */
    public void detectMongoVersion() {
        // Use ShardClient's built-in version detection
        useMoveRange = shardClient.isVersion6OrLater();
        String version = shardClient.getVersion();

        if (useMoveRange) {
            logger.info("Will use moveRange command (MongoDB {} >= 6.0)", version);
        } else {
            logger.info("Will use moveChunk command (MongoDB {} < 6.0)", version);
        }
    }

    /**
     * Move a chunk from source shard to destination shard.
     * Automatically handles ChunkTooBig errors by splitting and retrying.
     *
     * @param chunk the chunk to move
     * @param destShard destination shard ID
     * @return MoveChunkResult with success status and error details
     */
    public MoveChunkResult moveChunk(Megachunk chunk, String destShard) {
        String chunkKey = getChunkKey(chunk);

        // Check if chunk was recently moved
        Long minutesSinceMove = chunk.elapsedSinceLastMoved();
        if (minutesSinceMove != null) {
            if (minutesSinceMove < MINIMUM_MOVE_INTERVAL_MINUTES) {
                logger.debug("Skipping chunk {} - moved {} minutes ago (< {} min threshold)",
                           chunkKey, minutesSinceMove, MINIMUM_MOVE_INTERVAL_MINUTES);
                return MoveChunkResult.failure(String.format("Chunk was moved %d minutes ago (< %d min threshold)",
                    minutesSinceMove, MINIMUM_MOVE_INTERVAL_MINUTES));
            }
        }

        // Check if we've already moved this chunk in this session
        if (movedChunks.contains(chunkKey)) {
            logger.debug("Skipping chunk {} - already moved in this session", chunkKey);
            return MoveChunkResult.failure(String.format(
                "Chunk was already moved in this balancer session - chunk: %s, attempted move: %s -> %s",
                chunkKey, chunk.getShard(), destShard));
        }

        if (dryRun) {
            logger.info("[DRY-RUN] Would move chunk {} from {} to {}",
                       chunkKey, chunk.getShard(), destShard);
            movedChunks.add(chunkKey);
            return MoveChunkResult.success();
        }

        try {
            // For MongoDB 6.0+, pass null for max to let MongoDB automatically determine
            // an appropriate sub-range that fits within maxChunkSizeBytes.
            // This handles large auto-merged chunks without hitting ChunkTooBig errors.
            // For older versions, use both min and max to move the entire chunk.
            MoveChunkResult result = shardClient.moveChunkWithResult(
                chunk.getNs(),
                chunk.getMin(),
                useMoveRange ? null : chunk.getMax(),  // null max for 6.0+ = auto sub-range
                destShard,
                useSecondaryThrottle,
                waitForDelete,
                false,  // majorityWrite
                useMoveRange  // Use moveRange for MongoDB 6.0+
            );

            if (result.isSuccess()) {
                movedChunks.add(chunkKey);
                chunk.updateLastMovedTime();
                logger.info("Successfully moved chunk {} from {} to {}",
                          chunkKey, chunk.getShard(), destShard);
            }

            return result;

        } catch (MongoCommandException e) {
            if (e.getErrorCode() == CHUNK_TOO_BIG_ERROR_CODE) {
                logger.warn("Chunk {} is too big to move, attempting to split", chunkKey);
                return handleChunkTooBig(chunk, destShard);
            } else {
                logger.error("Failed to move chunk {}: {} (code: {})",
                           chunkKey, e.getErrorMessage(), e.getErrorCode());
                return MoveChunkResult.failure(e);
            }
        }
    }

    /**
     * Handle ChunkTooBig error by splitting the chunk and retrying the move.
     */
    private MoveChunkResult handleChunkTooBig(Megachunk chunk, String destShard) {
        try {
            // Try to split the chunk at a document
            logger.info("Attempting to split chunk {}", getChunkKey(chunk));

            Document splitResult = shardClient.splitFind(chunk.getNs(), chunk.getMin(), false);

            if (splitResult == null || splitResult.getDouble("ok") != 1.0) {
                logger.error("Failed to split chunk {}", getChunkKey(chunk));
                return MoveChunkResult.failure("Chunk too big - split failed");
            }

            logger.info("Successfully split chunk {}, reloading chunks", getChunkKey(chunk));

            // After split, the original chunk no longer exists, so we can't retry the move
            // The caller should refresh the chunk list and try again
            return MoveChunkResult.failure("Chunk too big - split succeeded, retry needed");

        } catch (Exception e) {
            logger.error("Failed to split chunk {}", getChunkKey(chunk), e);
            return MoveChunkResult.failure("Chunk too big - split failed: " + e.getMessage());
        }
    }

    /**
     * Get a unique key for this chunk for tracking purposes.
     */
    private String getChunkKey(Megachunk chunk) {
        return String.format("%s[%s-%s]", chunk.getNs(), chunk.getMin(), chunk.getMax());
    }

    /**
     * Reset the moved chunks tracker (e.g., at the start of a new balancing round).
     */
    public void resetMovedChunksTracker() {
        movedChunks.clear();
        logger.debug("Reset moved chunks tracker");
    }

    public boolean isUseMoveRange() {
        return useMoveRange;
    }

    public Set<String> getMovedChunks() {
        return new HashSet<>(movedChunks);
    }

    /**
     * Check if a chunk was already moved in this session.
     * More efficient than getMovedChunks() when only checking individual chunks.
     */
    public boolean wasChunkMoved(Megachunk chunk) {
        String chunkKey = getChunkKey(chunk);
        return movedChunks.contains(chunkKey);
    }
}
