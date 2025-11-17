package com.mongodb.balancer.model;

import com.mongodb.model.Megachunk;

/**
 * Represents a planned chunk migration from one shard to another.
 * Used to batch multiple migrations for parallel execution.
 */
public class PlannedMigration {

    private final Megachunk chunk;
    private final String sourceShard;
    private final String destinationShard;
    private boolean executed;
    private boolean success;
    private Exception error;

    public PlannedMigration(Megachunk chunk, String sourceShard, String destinationShard) {
        this.chunk = chunk;
        this.sourceShard = sourceShard;
        this.destinationShard = destinationShard;
        this.executed = false;
        this.success = false;
    }

    public Megachunk getChunk() {
        return chunk;
    }

    public String getSourceShard() {
        return sourceShard;
    }

    public String getDestinationShard() {
        return destinationShard;
    }

    public boolean isExecuted() {
        return executed;
    }

    public void setExecuted(boolean executed) {
        this.executed = executed;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public Exception getError() {
        return error;
    }

    public void setError(Exception error) {
        this.error = error;
    }

    @Override
    public String toString() {
        return String.format("PlannedMigration{chunk=%s, from=%s, to=%s, executed=%s, success=%s}",
                chunk.getNs() + "[" + chunk.getMin() + "-" + chunk.getMax() + "]",
                sourceShard, destinationShard, executed, success);
    }
}
