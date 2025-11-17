package com.mongodb.balancer.stats;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks balancer statistics for monitoring and reporting.
 */
public class BalancerStatistics {

    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicLong totalDurationMs = new AtomicLong(0);
    private final AtomicLong lastSuccessTime = new AtomicLong(0);
    private final long startTime;

    // Rolling window tracking
    private final RollingWindow hourlyWindow = new RollingWindow(60 * 60 * 1000); // 1 hour
    private final RollingWindow dailyWindow = new RollingWindow(24 * 60 * 60 * 1000); // 24 hours

    // Recent failures tracking
    private static final int MAX_RECENT_FAILURES = 10;
    private final FailureRecord[] recentFailures = new FailureRecord[MAX_RECENT_FAILURES];
    private int failureIndex = 0;

    public BalancerStatistics() {
        this.startTime = System.currentTimeMillis();
    }

    /**
     * Record of a failed migration.
     */
    public static class FailureRecord {
        private final long timestamp;
        private final String sourceShard;
        private final String destShard;
        private final String namespace;
        private final String errorMessage;

        public FailureRecord(String sourceShard, String destShard, String namespace, String errorMessage) {
            this.timestamp = System.currentTimeMillis();
            this.sourceShard = sourceShard;
            this.destShard = destShard;
            this.namespace = namespace;
            this.errorMessage = errorMessage;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getSourceShard() {
            return sourceShard;
        }

        public String getDestShard() {
            return destShard;
        }

        public String getNamespace() {
            return namespace;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public String getFormattedTimestamp() {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss")
                .withZone(ZoneId.systemDefault());
            return formatter.format(Instant.ofEpochMilli(timestamp));
        }
    }

    /**
     * Record a successful migration.
     */
    public void recordSuccess(long durationMs) {
        successCount.incrementAndGet();
        totalDurationMs.addAndGet(durationMs);
        lastSuccessTime.set(System.currentTimeMillis());
        hourlyWindow.recordEvent();
        dailyWindow.recordEvent();
    }

    /**
     * Record a failed migration.
     */
    public void recordFailure() {
        failureCount.incrementAndGet();
    }

    /**
     * Record a failed migration with details.
     */
    public synchronized void recordFailure(String sourceShard, String destShard, String namespace, String errorMessage) {
        failureCount.incrementAndGet();

        // Store in circular buffer
        recentFailures[failureIndex] = new FailureRecord(sourceShard, destShard, namespace, errorMessage);
        failureIndex = (failureIndex + 1) % MAX_RECENT_FAILURES;
    }

    /**
     * Get recent failures (up to last 10).
     */
    public synchronized List<FailureRecord> getRecentFailures() {
        List<FailureRecord> failures = new ArrayList<>();
        for (FailureRecord failure : recentFailures) {
            if (failure != null) {
                failures.add(failure);
            }
        }
        // Sort by timestamp descending (most recent first)
        failures.sort((a, b) -> Long.compare(b.getTimestamp(), a.getTimestamp()));
        return failures;
    }

    public int getSuccessCount() {
        return successCount.get();
    }

    public int getFailureCount() {
        return failureCount.get();
    }

    public int getHourlyCount() {
        return hourlyWindow.getCount();
    }

    public int getDailyCount() {
        return dailyWindow.getCount();
    }

    public long getAverageDurationMs() {
        int count = successCount.get();
        if (count == 0) {
            return 0;
        }
        return totalDurationMs.get() / count;
    }

    public long getLastSuccessTime() {
        return lastSuccessTime.get();
    }

    public long getMinutesSinceLastSuccess() {
        long last = lastSuccessTime.get();
        if (last == 0) {
            return -1;
        }
        return (System.currentTimeMillis() - last) / (60 * 1000);
    }

    public long getUptimeMinutes() {
        return (System.currentTimeMillis() - startTime) / (60 * 1000);
    }

    /**
     * Simple rolling window counter for time-based tracking.
     */
    private static class RollingWindow {
        private final long windowMs;
        private long[] timestamps = new long[1000]; // Circular buffer
        private int index = 0;

        public RollingWindow(long windowMs) {
            this.windowMs = windowMs;
        }

        public synchronized void recordEvent() {
            timestamps[index] = System.currentTimeMillis();
            index = (index + 1) % timestamps.length;
        }

        public synchronized int getCount() {
            long now = System.currentTimeMillis();
            long cutoff = now - windowMs;
            int count = 0;
            for (long ts : timestamps) {
                if (ts > cutoff) {
                    count++;
                }
            }
            return count;
        }
    }
}
