package com.mongodb.balancer.cache;

import com.mongodb.model.Megachunk;
import com.mongodb.shardsync.ShardClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Central registry for tracking chunk locations across all shards.
 * Maintains an in-memory cache of chunks and their current shard assignments.
 * Updates incrementally when chunks are migrated, with periodic full refreshes.
 */
public class ChunkRegistry {

    private static final Logger logger = LoggerFactory.getLogger(ChunkRegistry.class);

    private final ShardClient shardClient;
    private final long refreshIntervalMs;

    // Map: namespace -> (chunkKey -> ChunkInfo)
    private final Map<String, Map<String, ChunkInfo>> registry = new ConcurrentHashMap<>();

    // Reverse index: shardId -> Set<chunkKey>
    private final Map<String, Set<String>> shardToChunks = new ConcurrentHashMap<>();

    private volatile long lastFullRefreshTime = 0;

    public ChunkRegistry(ShardClient shardClient, long refreshIntervalMs) {
        this.shardClient = shardClient;
        this.refreshIntervalMs = refreshIntervalMs;
    }

    /**
     * Initialize the registry with a full load from MongoDB.
     */
    public synchronized void initialize() {
        logger.info("Initializing chunk registry with full load from MongoDB");
        fullRefresh();
    }

    /**
     * Get all movable chunks for a specific shard.
     */
    public List<Megachunk> getChunksForShard(String shardId) {
        refreshIfNeeded();

        Set<String> chunkKeys = shardToChunks.getOrDefault(shardId, Collections.emptySet());
        List<Megachunk> chunks = new ArrayList<>();

        for (String chunkKey : chunkKeys) {
            String[] parts = chunkKey.split(":", 2);
            if (parts.length != 2) continue;

            String namespace = parts[0];
            Map<String, ChunkInfo> nsChunks = registry.get(namespace);
            if (nsChunks != null) {
                ChunkInfo info = nsChunks.get(chunkKey);
                if (info != null) {
                    chunks.add(info.chunk);
                }
            }
        }

        logger.debug("Registry: Found {} chunks for shard {}", chunks.size(), shardId);
        return chunks;
    }

    /**
     * Record a chunk migration - update the registry without reloading from MongoDB.
     */
    public synchronized void recordMigration(String namespace, Megachunk chunk, String fromShard, String toShard) {
        String chunkKey = makeChunkKey(namespace, chunk);

        logger.debug("Registry: Recording migration {} from {} to {}", chunkKey, fromShard, toShard);

        // Update chunk info
        Map<String, ChunkInfo> nsChunks = registry.get(namespace);
        if (nsChunks != null) {
            ChunkInfo info = nsChunks.get(chunkKey);
            if (info != null) {
                info.currentShard = toShard;
            }
        }

        // Update reverse index
        Set<String> fromChunks = shardToChunks.get(fromShard);
        if (fromChunks != null) {
            fromChunks.remove(chunkKey);
        }

        shardToChunks.computeIfAbsent(toShard, k -> ConcurrentHashMap.newKeySet()).add(chunkKey);
    }

    /**
     * Refresh the registry if it's stale (older than refreshIntervalMs).
     */
    private void refreshIfNeeded() {
        long now = System.currentTimeMillis();
        if (now - lastFullRefreshTime > refreshIntervalMs) {
            synchronized (this) {
                // Double-check after acquiring lock
                if (now - lastFullRefreshTime > refreshIntervalMs) {
                    logger.info("Registry is stale ({}s old), performing full refresh",
                        (now - lastFullRefreshTime) / 1000);
                    fullRefresh();
                }
            }
        }
    }

    /**
     * Perform a full refresh from MongoDB - load all chunks for all shards.
     */
    private synchronized void fullRefresh() {
        long startTime = System.currentTimeMillis();

        // Clear existing data
        registry.clear();
        shardToChunks.clear();

        // Get all shards
        Set<String> shardIds = shardClient.getShardsMap().keySet();

        int totalChunks = 0;
        for (String shardId : shardIds) {
            try {
                // Load chunks for this shard
                List<Megachunk> chunks = com.mongodb.util.ChunkUtils.loadChunksForShard(
                    shardClient,
                    shardId,
                    true,  // excludeJumbo
                    true,  // excludeNoBalance
                    null   // no namespace filter
                );

                // Register each chunk
                for (Megachunk chunk : chunks) {
                    String namespace = chunk.getNs();
                    String chunkKey = makeChunkKey(namespace, chunk);

                    // Add to namespace map
                    registry.computeIfAbsent(namespace, k -> new ConcurrentHashMap<>())
                        .put(chunkKey, new ChunkInfo(chunk, shardId));

                    // Add to reverse index
                    shardToChunks.computeIfAbsent(shardId, k -> ConcurrentHashMap.newKeySet())
                        .add(chunkKey);
                }

                totalChunks += chunks.size();
                logger.debug("Registry: Loaded {} chunks for shard {}", chunks.size(), shardId);

            } catch (Exception e) {
                logger.error("Failed to load chunks for shard {}", shardId, e);
            }
        }

        lastFullRefreshTime = System.currentTimeMillis();
        long duration = lastFullRefreshTime - startTime;

        logger.info("Registry: Full refresh completed - {} chunks across {} shards in {}ms",
            totalChunks, shardIds.size(), duration);
    }

    /**
     * Invalidate the registry for specific shards - they'll be reloaded on next access.
     * This is a lighter-weight operation than full refresh.
     */
    public synchronized void invalidateShards(String... shardIds) {
        for (String shardId : shardIds) {
            Set<String> chunkKeys = shardToChunks.remove(shardId);
            if (chunkKeys != null) {
                logger.debug("Registry: Invalidated {} chunks for shard {}", chunkKeys.size(), shardId);

                // Remove these chunks from the namespace maps
                for (String chunkKey : chunkKeys) {
                    String[] parts = chunkKey.split(":", 2);
                    if (parts.length != 2) continue;

                    String namespace = parts[0];
                    Map<String, ChunkInfo> nsChunks = registry.get(namespace);
                    if (nsChunks != null) {
                        nsChunks.remove(chunkKey);
                    }
                }
            }
        }
    }

    /**
     * Make a unique key for a chunk.
     */
    private String makeChunkKey(String namespace, Megachunk chunk) {
        // Use namespace + min bound as the key
        return namespace + ":" + chunk.getMin().toString();
    }

    /**
     * Get statistics about the registry.
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("namespaces", registry.size());
        stats.put("totalChunks", registry.values().stream()
            .mapToInt(Map::size)
            .sum());
        stats.put("shards", shardToChunks.size());
        stats.put("lastRefreshAgeSeconds", (System.currentTimeMillis() - lastFullRefreshTime) / 1000);
        return stats;
    }

    /**
     * Internal class to hold chunk information.
     */
    private static class ChunkInfo {
        final Megachunk chunk;
        volatile String currentShard;

        ChunkInfo(Megachunk chunk, String currentShard) {
            this.chunk = chunk;
            this.currentShard = currentShard;
        }
    }
}
