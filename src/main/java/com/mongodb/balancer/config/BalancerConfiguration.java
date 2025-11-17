package com.mongodb.balancer.config;

import com.mongodb.shardsync.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration for the strategy-based balancer.
 * Extends BaseConfiguration to inherit MongoDB connection and filter settings.
 */
public class BalancerConfiguration extends BaseConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(BalancerConfiguration.class);

    // Strategy configuration
    private String[] strategies;              // e.g., ["diskSpace:1.0", "dataSize:0.3", "chunkCount:0.1"]
    private String chunkSelectionStrategy;    // e.g., "random", "largest", "smallest"

    // Rate limiting
    private int maxChunksPerHour;
    private int maxChunksPerDay;
    private long sleepBetweenMovesMs;

    // Metrics collection
    private long metricsRefreshIntervalMs;    // How often to refresh metrics
    private long statusReportIntervalMs;      // How often to print status reports

    // Move options
    private boolean useSecondaryThrottle;
    private boolean waitForDelete;
    private boolean disableBuiltInBalancer;
    private boolean dryRun;

    // Balancing loop
    private boolean continuousMode;           // Keep running in a loop

    // Zone and validation
    private boolean requireNoZones;           // Fail if zones are detected
    private boolean respectZones;             // Respect zone constraints (future use)

    // Thresholds
    private long maxChunkSizeBytes;           // Maximum chunk size for 3x threshold rule
    private double thresholdMultiplier;       // Multiplier for threshold check (default 3.0)
    private long thresholdBytes;              // Absolute threshold in bytes (e.g., 100MB, 1GB)

    // Namespace-focused balancing
    private boolean useNamespaceFocus;        // Enable namespace-level metrics and focused balancing
    private long namespaceFullScanIntervalMs; // How often to scan all namespaces (default 30 minutes)
    private int maxActiveNamespaces;          // Max number of namespaces to actively balance (default 10)
    private double namespaceImbalanceThreshold; // Imbalance score threshold (default 0.1 = 10% variation)

    public BalancerConfiguration() {
        // Default values
        this.strategies = new String[]{"dataSize:1.0"};  // Use dataSize instead of diskSpace (storage doesn't get released automatically)
        this.chunkSelectionStrategy = "random";
        this.maxChunksPerHour = 20;
        this.maxChunksPerDay = 400;
        this.sleepBetweenMovesMs = 500;  // 500 milliseconds
        this.metricsRefreshIntervalMs = 300000;  // 5 minutes
        this.statusReportIntervalMs = 60000;  // 60 seconds
        this.useSecondaryThrottle = true;
        this.waitForDelete = true;
        this.disableBuiltInBalancer = true;
        this.dryRun = false;
        this.continuousMode = true;
        this.requireNoZones = true;  // Fail-safe default
        this.respectZones = true;    // Future use
        this.maxChunkSizeBytes = 64L * 1024 * 1024;  // 64MB default
        this.thresholdMultiplier = 3.0;  // MongoDB's 3x rule
        this.thresholdBytes = 100L * 1024 * 1024;  // 100MB default
        this.useNamespaceFocus = false;  // Disabled by default
        this.namespaceFullScanIntervalMs = 1800000;  // 30 minutes
        this.maxActiveNamespaces = 10;  // Balance up to 10 namespaces concurrently
        this.namespaceImbalanceThreshold = 0.1;  // 10% variation threshold (lowered from 30%)
    }

    // Getters and setters

    public String[] getStrategies() {
        return strategies;
    }

    public void setStrategies(String[] strategies) {
        this.strategies = strategies;
    }

    public String getChunkSelectionStrategy() {
        return chunkSelectionStrategy;
    }

    public void setChunkSelectionStrategy(String chunkSelectionStrategy) {
        this.chunkSelectionStrategy = chunkSelectionStrategy;
    }

    public int getMaxChunksPerHour() {
        return maxChunksPerHour;
    }

    public void setMaxChunksPerHour(int maxChunksPerHour) {
        this.maxChunksPerHour = maxChunksPerHour;
    }

    public int getMaxChunksPerDay() {
        return maxChunksPerDay;
    }

    public void setMaxChunksPerDay(int maxChunksPerDay) {
        this.maxChunksPerDay = maxChunksPerDay;
    }

    public long getSleepBetweenMovesMs() {
        return sleepBetweenMovesMs;
    }

    public void setSleepBetweenMovesMs(long sleepBetweenMovesMs) {
        this.sleepBetweenMovesMs = sleepBetweenMovesMs;
    }

    public long getMetricsRefreshIntervalMs() {
        return metricsRefreshIntervalMs;
    }

    public void setMetricsRefreshIntervalMs(long metricsRefreshIntervalMs) {
        this.metricsRefreshIntervalMs = metricsRefreshIntervalMs;
    }

    public long getStatusReportIntervalMs() {
        return statusReportIntervalMs;
    }

    public void setStatusReportIntervalMs(long statusReportIntervalMs) {
        this.statusReportIntervalMs = statusReportIntervalMs;
    }

    public boolean isUseSecondaryThrottle() {
        return useSecondaryThrottle;
    }

    public void setUseSecondaryThrottle(boolean useSecondaryThrottle) {
        this.useSecondaryThrottle = useSecondaryThrottle;
    }

    public boolean isWaitForDelete() {
        return waitForDelete;
    }

    public void setWaitForDelete(boolean waitForDelete) {
        this.waitForDelete = waitForDelete;
    }

    public boolean isDisableBuiltInBalancer() {
        return disableBuiltInBalancer;
    }

    public void setDisableBuiltInBalancer(boolean disableBuiltInBalancer) {
        this.disableBuiltInBalancer = disableBuiltInBalancer;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public boolean isContinuousMode() {
        return continuousMode;
    }

    public void setContinuousMode(boolean continuousMode) {
        this.continuousMode = continuousMode;
    }

    public boolean isRequireNoZones() {
        return requireNoZones;
    }

    public void setRequireNoZones(boolean requireNoZones) {
        this.requireNoZones = requireNoZones;
    }

    public boolean isRespectZones() {
        return respectZones;
    }

    public void setRespectZones(boolean respectZones) {
        this.respectZones = respectZones;
    }

    public long getMaxChunkSizeBytes() {
        return maxChunkSizeBytes;
    }

    public void setMaxChunkSizeBytes(long maxChunkSizeBytes) {
        this.maxChunkSizeBytes = maxChunkSizeBytes;
    }

    public double getThresholdMultiplier() {
        return thresholdMultiplier;
    }

    public void setThresholdMultiplier(double thresholdMultiplier) {
        this.thresholdMultiplier = thresholdMultiplier;
    }

    public long getThresholdBytes() {
        return thresholdBytes;
    }

    public void setThresholdBytes(long thresholdBytes) {
        this.thresholdBytes = thresholdBytes;
    }

    public boolean isUseNamespaceFocus() {
        return useNamespaceFocus;
    }

    public void setUseNamespaceFocus(boolean useNamespaceFocus) {
        this.useNamespaceFocus = useNamespaceFocus;
    }

    public long getNamespaceFullScanIntervalMs() {
        return namespaceFullScanIntervalMs;
    }

    public void setNamespaceFullScanIntervalMs(long namespaceFullScanIntervalMs) {
        this.namespaceFullScanIntervalMs = namespaceFullScanIntervalMs;
    }

    public int getMaxActiveNamespaces() {
        return maxActiveNamespaces;
    }

    public void setMaxActiveNamespaces(int maxActiveNamespaces) {
        this.maxActiveNamespaces = maxActiveNamespaces;
    }

    public double getNamespaceImbalanceThreshold() {
        return namespaceImbalanceThreshold;
    }

    public void setNamespaceImbalanceThreshold(double namespaceImbalanceThreshold) {
        this.namespaceImbalanceThreshold = namespaceImbalanceThreshold;
    }

    /**
     * Parse a threshold string like "100MB", "1GB", "0.5GB", "0.05TB" to bytes.
     */
    public static long parseThreshold(String threshold) {
        if (threshold == null || threshold.isEmpty()) {
            throw new IllegalArgumentException("Threshold cannot be null or empty");
        }

        String upper = threshold.toUpperCase().trim();
        double value;
        long multiplier;

        if (upper.endsWith("TB")) {
            value = Double.parseDouble(upper.substring(0, upper.length() - 2));
            multiplier = 1024L * 1024 * 1024 * 1024;
        } else if (upper.endsWith("GB")) {
            value = Double.parseDouble(upper.substring(0, upper.length() - 2));
            multiplier = 1024L * 1024 * 1024;
        } else if (upper.endsWith("MB")) {
            value = Double.parseDouble(upper.substring(0, upper.length() - 2));
            multiplier = 1024L * 1024;
        } else {
            throw new IllegalArgumentException("Threshold must end with MB, GB, or TB: " + threshold);
        }

        return (long) (value * multiplier);
    }

    /**
     * Load configuration from a properties file.
     */
    public void loadPropertiesFile(String configFile) throws ConfigurationException {
        logger.info("Loading properties from file: {}", configFile);

        FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
            new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
                .configure(new Parameters().properties()
                    .setFileName(configFile)
                    .setThrowExceptionOnMissing(true)
                    .setListDelimiterHandler(new DefaultListDelimiterHandler(','))
                    .setIncludesAllowed(false));

        Configuration config = builder.getConfiguration();

        // Load inherited BaseConfiguration properties
        this.sourceClusterUri = config.getString("sourceClusterUri");

        // Load balancer-specific properties
        this.strategies = config.getStringArray("strategies");
        this.chunkSelectionStrategy = config.getString("chunkSelectionStrategy", "random");
        this.maxChunksPerHour = config.getInt("maxChunksPerHour", 20);
        this.maxChunksPerDay = config.getInt("maxChunksPerDay", 400);
        this.sleepBetweenMovesMs = config.getLong("sleepBetweenMovesMs", 500L);
        this.metricsRefreshIntervalMs = config.getLong("metricsRefreshIntervalMs", 300000L);
        this.statusReportIntervalMs = config.getLong("statusReportIntervalMs", 60000L);
        this.useSecondaryThrottle = config.getBoolean("useSecondaryThrottle", true);
        this.waitForDelete = config.getBoolean("waitForDelete", true);
        this.disableBuiltInBalancer = config.getBoolean("disableBuiltInBalancer", true);
        this.dryRun = config.getBoolean("dryRun", false);
        this.continuousMode = config.getBoolean("continuousMode", true);
        this.requireNoZones = config.getBoolean("requireNoZones", true);
        this.respectZones = config.getBoolean("respectZones", true);
        this.maxChunkSizeBytes = config.getLong("maxChunkSizeBytes", 64L * 1024 * 1024);
        this.thresholdMultiplier = config.getDouble("thresholdMultiplier", 3.0);

        // Load threshold (supports both string format like "100MB" or direct bytes)
        String thresholdStr = config.getString("threshold");
        if (thresholdStr != null) {
            this.thresholdBytes = parseThreshold(thresholdStr);
        }

        // Load namespace filters if specified
        String[] includeNamespaces = config.getStringArray("includeNamespaces");
        if (includeNamespaces != null && includeNamespaces.length > 0) {
            setNamespaceFilters(includeNamespaces);
        }

        String[] includeDatabases = config.getStringArray("includeDatabases");
        if (includeDatabases != null && includeDatabases.length > 0) {
            setIncludeDatabases(includeDatabases);
        }

        // Load namespace-focused balancing configuration
        this.useNamespaceFocus = config.getBoolean("useNamespaceFocus", false);
        this.namespaceFullScanIntervalMs = config.getLong("namespaceFullScanIntervalMs", 1800000L);
        this.maxActiveNamespaces = config.getInt("maxActiveNamespaces", 10);
        this.namespaceImbalanceThreshold = config.getDouble("namespaceImbalanceThreshold", 0.1);

        logger.info("Configuration loaded successfully");
    }
}
