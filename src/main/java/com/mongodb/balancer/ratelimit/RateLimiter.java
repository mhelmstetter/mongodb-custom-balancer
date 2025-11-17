package com.mongodb.balancer.ratelimit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Rate limiter for chunk moves to avoid overwhelming the cluster.
 * Supports hourly and daily limits, plus configurable sleep between moves.
 */
public class RateLimiter {

    private static final Logger logger = LoggerFactory.getLogger(RateLimiter.class);

    private final int maxChunksPerHour;
    private final int maxChunksPerDay;
    private final long sleepBetweenMovesMs;

    private int chunksMovedThisHour;
    private int chunksMovedToday;
    private LocalDateTime currentHourStart;
    private LocalDateTime currentDayStart;
    private long lastMoveTime;

    public RateLimiter(int maxChunksPerHour, int maxChunksPerDay, long sleepBetweenMovesMs) {
        this.maxChunksPerHour = maxChunksPerHour;
        this.maxChunksPerDay = maxChunksPerDay;
        this.sleepBetweenMovesMs = sleepBetweenMovesMs;
        this.chunksMovedThisHour = 0;
        this.chunksMovedToday = 0;
        this.currentHourStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS);
        this.currentDayStart = LocalDateTime.now().truncatedTo(ChronoUnit.DAYS);
        this.lastMoveTime = 0;
    }

    /**
     * Check if a move can proceed based on rate limits.
     * Returns false if limits have been reached.
     */
    public boolean canMove() {
        resetCountersIfNeeded();

        if (maxChunksPerHour > 0 && chunksMovedThisHour >= maxChunksPerHour) {
            logger.info("Hourly limit reached: {}/{} chunks moved this hour",
                       chunksMovedThisHour, maxChunksPerHour);
            return false;
        }

        if (maxChunksPerDay > 0 && chunksMovedToday >= maxChunksPerDay) {
            logger.info("Daily limit reached: {}/{} chunks moved today",
                       chunksMovedToday, maxChunksPerDay);
            return false;
        }

        return true;
    }

    /**
     * Record that a chunk was moved and sleep if configured.
     * Should be called after a successful move.
     */
    public void recordMove() {
        resetCountersIfNeeded();

        chunksMovedThisHour++;
        chunksMovedToday++;
        lastMoveTime = System.currentTimeMillis();

        logger.debug("Chunks moved: {}/{} this hour, {}/{} today",
                   chunksMovedThisHour, maxChunksPerHour > 0 ? maxChunksPerHour : "unlimited",
                   chunksMovedToday, maxChunksPerDay > 0 ? maxChunksPerDay : "unlimited");
    }

    /**
     * Sleep between moves if configured.
     * Should be called after recordMove().
     */
    public void sleepBetweenMoves() {
        if (sleepBetweenMovesMs > 0) {
            try {
                logger.debug("Sleeping {} ms between moves", sleepBetweenMovesMs);
                Thread.sleep(sleepBetweenMovesMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Sleep interrupted", e);
            }
        }
    }

    /**
     * Wait until the rate limit allows another move.
     * Blocks until the next hour or day starts if limits are reached.
     */
    public void waitForRateLimitReset() {
        resetCountersIfNeeded();

        if (maxChunksPerHour > 0 && chunksMovedThisHour >= maxChunksPerHour) {
            LocalDateTime nextHour = currentHourStart.plusHours(1);
            long sleepMs = ChronoUnit.MILLIS.between(LocalDateTime.now(), nextHour);
            if (sleepMs > 0) {
                logger.info("Hourly limit reached, waiting {} ms until next hour", sleepMs);
                try {
                    Thread.sleep(sleepMs);
                    resetCountersIfNeeded();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Sleep interrupted", e);
                }
            }
        } else if (maxChunksPerDay > 0 && chunksMovedToday >= maxChunksPerDay) {
            LocalDateTime nextDay = currentDayStart.plusDays(1);
            long sleepMs = ChronoUnit.MILLIS.between(LocalDateTime.now(), nextDay);
            if (sleepMs > 0) {
                logger.info("Daily limit reached, waiting {} ms until next day", sleepMs);
                try {
                    Thread.sleep(sleepMs);
                    resetCountersIfNeeded();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Sleep interrupted", e);
                }
            }
        }
    }

    /**
     * Reset hourly/daily counters if the time period has passed.
     */
    private void resetCountersIfNeeded() {
        LocalDateTime now = LocalDateTime.now();

        // Check if we've moved to a new hour
        LocalDateTime currentHourTruncated = now.truncatedTo(ChronoUnit.HOURS);
        if (currentHourTruncated.isAfter(currentHourStart)) {
            logger.debug("New hour started, resetting hourly counter (was {})", chunksMovedThisHour);
            chunksMovedThisHour = 0;
            currentHourStart = currentHourTruncated;
        }

        // Check if we've moved to a new day
        LocalDateTime currentDayTruncated = now.truncatedTo(ChronoUnit.DAYS);
        if (currentDayTruncated.isAfter(currentDayStart)) {
            logger.info("New day started, resetting daily counter (was {})", chunksMovedToday);
            chunksMovedToday = 0;
            currentDayStart = currentDayTruncated;
        }
    }

    public int getChunksMovedThisHour() {
        resetCountersIfNeeded();
        return chunksMovedThisHour;
    }

    public int getChunksMovedToday() {
        resetCountersIfNeeded();
        return chunksMovedToday;
    }

    public int getMaxChunksPerHour() {
        return maxChunksPerHour;
    }

    public int getMaxChunksPerDay() {
        return maxChunksPerDay;
    }

    public long getSleepBetweenMovesMs() {
        return sleepBetweenMovesMs;
    }
}
