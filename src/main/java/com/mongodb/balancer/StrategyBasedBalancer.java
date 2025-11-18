package com.mongodb.balancer;

import com.mongodb.balancer.cache.ChunkRegistry;
import com.mongodb.balancer.config.BalancerConfiguration;
import com.mongodb.balancer.model.PlannedMigration;
import com.mongodb.balancer.metrics.DbStatsMetricsCollector;
import com.mongodb.balancer.metrics.MetricsCollector;
import com.mongodb.balancer.metrics.ShardMetrics;
import com.mongodb.balancer.mover.ChunkMover;
import com.mongodb.balancer.ratelimit.RateLimiter;
import com.mongodb.shardsync.MoveChunkResult;
import com.mongodb.balancer.selection.ChunkSelectionStrategy;
import com.mongodb.balancer.selection.LargestChunkSelector;
import com.mongodb.balancer.selection.RandomChunkSelector;
import com.mongodb.balancer.selection.SmallestChunkSelector;
import com.mongodb.balancer.strategy.BalancingStrategy;
import com.mongodb.balancer.strategy.ChunkCountBalancingStrategy;
import com.mongodb.balancer.strategy.DataSizeBalancingStrategy;
import com.mongodb.model.Megachunk;
import com.mongodb.shardsync.ChunkManager;
import com.mongodb.shardsync.ShardClient;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Main strategy-based balancer implementation.
 * Uses weighted scoring from multiple strategies to determine chunk moves.
 */
@Command(name = "mongodb-strategy-balancer", mixinStandardHelpOptions = true,
        version = "1.0.0", description = "Strategy-based MongoDB shard balancer",
        defaultValueProvider = CommandLine.PropertiesDefaultProvider.class)
public class StrategyBasedBalancer implements Callable<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(StrategyBasedBalancer.class);

    @Option(names = {"-c", "--config"}, description = "Configuration file path",
            defaultValue = "balancer.properties")
    private String configFile;

    // MongoDB connection
    @Option(names = {"-s", "--sourceClusterUri"}, description = "MongoDB cluster connection URI (mongos)")
    private String sourceClusterUri;

    // Strategy configuration
    @Option(names = {"--strategies"}, split = ",", description = "Comma-separated list of strategies with weights (format: strategy:weight)")
    private String[] strategiesConfig;

    @Option(names = {"--chunkSelectionStrategy"}, description = "Chunk selection strategy: random, largest, smallest")
    private String chunkSelectionStrategy;

    // Rate limiting
    @Option(names = {"--maxChunksPerHour"}, description = "Maximum chunks to move per hour")
    private Integer maxChunksPerHour;

    @Option(names = {"--maxChunksPerDay"}, description = "Maximum chunks to move per day")
    private Integer maxChunksPerDay;

    @Option(names = {"--sleepBetweenMovesMs"}, description = "Sleep duration between chunk moves in milliseconds")
    private Long sleepBetweenMovesMs;

    @Option(names = {"--continuousMode"}, description = "Run continuously in a loop (true) or run once and exit (false)")
    private Boolean continuousMode;

    // Metrics collection
    @Option(names = {"--metricsRefreshIntervalMs"}, description = "How often to refresh shard metrics in milliseconds")
    private Long metricsRefreshIntervalMs;

    @Option(names = {"--statusReportIntervalMs"}, description = "How often to print status reports in milliseconds (default: 60000)")
    private Long statusReportIntervalMs;

    // Move options
    @Option(names = {"--useSecondaryThrottle"}, description = "Use secondary throttle during chunk moves")
    private Boolean useSecondaryThrottle;

    @Option(names = {"--waitForDelete"}, description = "Wait for range deletion to complete after move")
    private Boolean waitForDelete;

    @Option(names = {"--disableBuiltInBalancer"}, description = "Disable MongoDB's built-in balancer")
    private Boolean disableBuiltInBalancer;

    @Option(names = {"--dryRun"}, description = "Dry run mode - log what would be done without actually moving chunks")
    private Boolean dryRun;

    @Option(names = {"-v", "--verbose"}, description = "Enable verbose debug logging")
    private Boolean verbose;

    // Zone and validation
    @Option(names = {"--requireNoZones"}, description = "Require that no zones are configured (fail-safe)")
    private Boolean requireNoZones;

    @Option(names = {"--respectZones"}, description = "Respect zone constraints when balancing (future use)")
    private Boolean respectZones;

    // Thresholds
    @Option(names = {"--maxChunkSizeBytes"}, description = "Maximum chunk size in bytes (used for 3x threshold rule)")
    private Long maxChunkSizeBytes;

    @Option(names = {"--thresholdMultiplier"}, description = "Threshold multiplier for balancing decisions")
    private Double thresholdMultiplier;

    @Option(names = {"--threshold"}, description = "Balancing threshold (e.g., 100MB, 1GB, 0.5GB, 0.05TB)")
    private String threshold;

    // Namespace filters
    @Option(names = {"--includeNamespaces"}, split = ",", description = "Include specific namespace(s) (format: db.collection)")
    private String[] includeNamespaces;

    @Option(names = {"--includeDatabases"}, split = ",", description = "Include specific database(s)")
    private String[] includeDatabases;

    private BalancerConfiguration config;
    private ShardClient shardClient;
    private MetricsCollector metricsCollector;
    private ChunkMover chunkMover;
    private RateLimiter rateLimiter;
    private ChunkSelectionStrategy chunkSelector;
    private List<BalancingStrategy> strategies;
    private ExecutorService migrationExecutor;
    private ExecutorService statusReporter;
    private ChunkRegistry chunkRegistry;

    // Namespace-focused balancing (optional)
    private com.mongodb.balancer.metrics.NamespaceMetricsCollector namespaceCollector;
    private List<String> activeNamespaces = new ArrayList<>();  // Namespaces currently being balanced
    private long lastNamespaceFullScanTime = 0;

    // Continuous balancing state
    private final Object balancingLock = new Object();
    private Set<String> inUseShards = new HashSet<>();
    private Set<String> inUseNamespaces = new HashSet<>();  // MongoDB allows only 1 migration per namespace
    private List<ShardMetrics> cachedMetrics = new ArrayList<>();
    private Map<String, Double> cachedScores = new HashMap<>();
    private long lastMetricsRefreshTime = 0;
    private volatile boolean running = false;

    // Statistics tracking
    private final com.mongodb.balancer.stats.BalancerStatistics statistics =
        new com.mongodb.balancer.stats.BalancerStatistics();

    public static void main(String[] args) {
        StrategyBasedBalancer app = new StrategyBasedBalancer();

        try {
            // First parse to extract config file path
            CommandLine tempCmd = new CommandLine(new StrategyBasedBalancer());
            CommandLine.ParseResult tempResult = tempCmd.parseArgs(args);

            File defaultsFile;
            StrategyBasedBalancer tempApp = tempResult.commandSpec().commandLine().getCommand();
            if (tempApp.configFile != null) {
                defaultsFile = new File(tempApp.configFile);
            } else {
                defaultsFile = new File("balancer.properties");
            }

            // Second parse with defaults loaded from file if it exists
            CommandLine cmd = new CommandLine(app);
            if (defaultsFile.exists()) {
                logger.info("Loading defaults from: {}", defaultsFile.getAbsolutePath());
                cmd.setDefaultValueProvider(new CommandLine.PropertiesDefaultProvider(defaultsFile));
            } else {
                logger.info("Configuration file {} not found, using command-line options and defaults",
                           defaultsFile.getAbsolutePath());
            }

            int exitCode = cmd.execute(args);
            System.exit(exitCode);

        } catch (Exception e) {
            logger.error("Error executing balancer", e);
            System.exit(1);
        }
    }

    @Override
    public Integer call() throws Exception {
        logger.info("=".repeat(80));
        logger.info("MongoDB Strategy-Based Balancer v1.0.0");
        logger.info("=".repeat(80));

        // Load configuration
        if (!loadConfiguration()) {
            return 1;
        }

        // Initialize components
        if (!initialize()) {
            return 1;
        }

        // Run balancer
        try {
            if (config.isContinuousMode()) {
                runContinuous();
            } else {
                runOnce();
            }
            return 0;
        } catch (Exception e) {
            logger.error("Balancer failed with exception", e);
            return 1;
        } finally {
            cleanup();
        }
    }

    /**
     * Load configuration from command-line options and/or properties file.
     */
    private boolean loadConfiguration() {
        try {
            logger.info("Loading configuration...");

            config = new BalancerConfiguration();
            File file = new File(configFile);

            // If file exists, load it first (provides base defaults)
            if (file.exists()) {
                logger.info("Loading configuration from file: {}", configFile);
                config.loadPropertiesFile(configFile);
            } else {
                logger.info("Configuration file not found: {}, using command-line options", configFile);
            }

            // Override with command-line options (if provided)
            if (sourceClusterUri != null) {
                config.setSourceClusterUri(sourceClusterUri);
            }
            if (strategiesConfig != null) {
                config.setStrategies(strategiesConfig);
            }
            if (chunkSelectionStrategy != null) {
                config.setChunkSelectionStrategy(chunkSelectionStrategy);
            }
            if (maxChunksPerHour != null) {
                config.setMaxChunksPerHour(maxChunksPerHour);
            }
            if (maxChunksPerDay != null) {
                config.setMaxChunksPerDay(maxChunksPerDay);
            }
            if (sleepBetweenMovesMs != null) {
                config.setSleepBetweenMovesMs(sleepBetweenMovesMs);
            }
            if (metricsRefreshIntervalMs != null) {
                config.setMetricsRefreshIntervalMs(metricsRefreshIntervalMs);
            }
            if (statusReportIntervalMs != null) {
                config.setStatusReportIntervalMs(statusReportIntervalMs);
            }
            if (useSecondaryThrottle != null) {
                config.setUseSecondaryThrottle(useSecondaryThrottle);
            }
            if (waitForDelete != null) {
                config.setWaitForDelete(waitForDelete);
            }
            if (disableBuiltInBalancer != null) {
                config.setDisableBuiltInBalancer(disableBuiltInBalancer);
            }
            if (dryRun != null) {
                config.setDryRun(dryRun);
            }
            if (verbose != null) {
                config.setVerbose(verbose);
            }
            if (continuousMode != null) {
                config.setContinuousMode(continuousMode);
            }
            if (requireNoZones != null) {
                config.setRequireNoZones(requireNoZones);
            }
            if (respectZones != null) {
                config.setRespectZones(respectZones);
            }
            if (maxChunkSizeBytes != null) {
                config.setMaxChunkSizeBytes(maxChunkSizeBytes);
            }
            if (thresholdMultiplier != null) {
                config.setThresholdMultiplier(thresholdMultiplier);
            }
            if (threshold != null) {
                config.setThresholdBytes(BalancerConfiguration.parseThreshold(threshold));
            }
            if (includeNamespaces != null && includeNamespaces.length > 0) {
                config.setIncludeNamespaces(includeNamespaces);
            }
            if (includeDatabases != null && includeDatabases.length > 0) {
                config.setIncludeDatabases(includeDatabases);
            }

            // Validate required configuration
            if (config.getSourceClusterUri() == null) {
                logger.error("sourceClusterUri is required. Provide it via command-line (-s) or properties file.");
                return false;
            }

            // Set logging level if verbose mode is enabled
            if (config.isVerbose()) {
                setLogLevel("DEBUG");
                logger.info("Verbose logging enabled (DEBUG level)");
            }

            logConfiguration();
            return true;

        } catch (Exception e) {
            logger.error("Failed to load configuration", e);
            return false;
        }
    }

    /**
     * Initialize all components.
     */
    private boolean initialize() {
        try {
            logger.info("Initializing balancer components...");

            // Initialize ShardClient
            shardClient = config.getSourceShardClient();
            shardClient.init();

            if (!shardClient.isMongos()) {
                logger.error("Source cluster must be a mongos");
                return false;
            }

            // Populate shard MongoClient connections
            logger.info("Connecting to {} shards...", shardClient.getShardsMap().size());
            shardClient.populateShardMongoClients();
            logger.info("Connected to {} shards", shardClient.getShardMongoClients().size());

            // Disable built-in balancer if configured
            if (config.isDisableBuiltInBalancer()) {
                logger.info("Disabling MongoDB built-in balancer");
                shardClient.stopBalancer();
            }

            // Initialize metrics collector
            metricsCollector = new DbStatsMetricsCollector(shardClient);

            // Initialize namespace-focused balancing if enabled
            if (config.isUseNamespaceFocus()) {
                logger.info("Namespace-focused balancing enabled (imbalance threshold: {})",
                    config.getNamespaceImbalanceThreshold());
                namespaceCollector = new com.mongodb.balancer.metrics.NamespaceMetricsCollector(
                    shardClient, config.getNamespaceImbalanceThreshold());

                // Perform initial full namespace scan to identify imbalanced namespaces
                logger.info("Performing initial namespace scan...");
                activeNamespaces = namespaceCollector.scanAllNamespaces();
                lastNamespaceFullScanTime = System.currentTimeMillis();

                // Limit to maxActiveNamespaces
                if (activeNamespaces.size() > config.getMaxActiveNamespaces()) {
                    logger.info("Limiting active namespaces from {} to {}",
                        activeNamespaces.size(), config.getMaxActiveNamespaces());
                    activeNamespaces = activeNamespaces.subList(0, config.getMaxActiveNamespaces());
                }

                logger.info("Focusing on {} imbalanced namespaces", activeNamespaces.size());
            }

            // Initialize chunk mover
            chunkMover = new ChunkMover(
                shardClient,
                config.isUseSecondaryThrottle(),
                config.isWaitForDelete(),
                config.isDryRun()
            );
            chunkMover.detectMongoVersion();

            // Initialize rate limiter
            rateLimiter = new RateLimiter(
                config.getMaxChunksPerHour(),
                config.getMaxChunksPerDay(),
                config.getSleepBetweenMovesMs()
            );

            // Initialize strategies
            strategies = loadStrategies();
            if (strategies.isEmpty()) {
                logger.error("No balancing strategies configured");
                return false;
            }

            // Initialize chunk selector
            chunkSelector = loadChunkSelector();

            // Generate initial shard metrics if using namespace-focus
            if (config.isUseNamespaceFocus() && namespaceCollector != null) {
                List<ShardMetrics> initialMetrics = namespaceCollector.generateShardMetrics();
                synchronized (balancingLock) {
                    cachedMetrics = initialMetrics;
                    cachedScores = calculateWeightedScores(cachedMetrics);
                    lastMetricsRefreshTime = System.currentTimeMillis();
                }
                logger.info("Generated shard metrics from {} namespaces", namespaceCollector.getAllNamespaceMetrics().size());
            }

            // Validate no zones (critical safety check)
            boolean hasZones = com.mongodb.util.ChunkUtils.hasZones(shardClient);
            if (hasZones) {
                logger.error("Cluster has zone tags configured. This balancer does not support zone-aware balancing.");

                if (config.isRequireNoZones()) {
                    logger.error("Set requireNoZones=false in configuration to bypass this check (NOT RECOMMENDED)");
                    return false;
                }

                logger.warn("Proceeding with balancing despite zones - THIS MAY VIOLATE ZONE CONSTRAINTS!");
            } else {
                logger.info("No zones detected - safe to proceed");
            }

            // Initialize migration executor
            // Size based on max concurrent migrations: floor(numShards/2)
            int maxConcurrentMigrations = Math.max(1, shardClient.getShardMongoClients().size() / 2);
            migrationExecutor = Executors.newFixedThreadPool(maxConcurrentMigrations);

            // Initialize chunk registry
            logger.info("Initializing chunk registry");
            chunkRegistry = new ChunkRegistry(shardClient, config.getMetricsRefreshIntervalMs());
            chunkRegistry.initialize();

            logger.info("Initialization complete - max concurrent migrations: {}", maxConcurrentMigrations);
            return true;

        } catch (Exception e) {
            logger.error("Failed to initialize balancer", e);
            return false;
        }
    }

    /**
     * Load balancing strategies from configuration.
     */
    private List<BalancingStrategy> loadStrategies() {
        List<BalancingStrategy> loadedStrategies = new ArrayList<>();
        String[] strategyConfigs = config.getStrategies();

        logger.info("Loading {} balancing strategies", strategyConfigs.length);

        for (String strategyConfig : strategyConfigs) {
            String[] parts = strategyConfig.split(":");
            if (parts.length != 2) {
                logger.warn("Invalid strategy config: {}, expected format 'name:weight'", strategyConfig);
                continue;
            }

            String name = parts[0].trim();
            double weight;
            try {
                weight = Double.parseDouble(parts[1].trim());
            } catch (NumberFormatException e) {
                logger.warn("Invalid weight for strategy {}: {}", name, parts[1]);
                continue;
            }

            BalancingStrategy strategy = createStrategy(name, weight);
            if (strategy != null) {
                loadedStrategies.add(strategy);
                logger.info("  Loaded strategy: {} with weight {}", name, weight);
            } else {
                logger.warn("Unknown strategy: {}", name);
            }
        }

        return loadedStrategies;
    }

    /**
     * Create a strategy instance by name.
     */
    private BalancingStrategy createStrategy(String name, double weight) {
        switch (name.toLowerCase()) {
            case "datasize":
                return new DataSizeBalancingStrategy(weight);
            case "chunkcount":
                return new ChunkCountBalancingStrategy(weight);
            default:
                return null;
        }
    }

    /**
     * Load chunk selection strategy from configuration.
     */
    private ChunkSelectionStrategy loadChunkSelector() {
        String selectorName = config.getChunkSelectionStrategy();
        logger.info("Loading chunk selector: {}", selectorName);

        switch (selectorName.toLowerCase()) {
            case "random":
                return new RandomChunkSelector();
            case "largest":
                return new LargestChunkSelector(shardClient);
            case "smallest":
                return new SmallestChunkSelector(shardClient);
            default:
                logger.warn("Unknown chunk selector: {}, using random", selectorName);
                return new RandomChunkSelector();
        }
    }

    /**
     * Run balancer continuously in a loop with worker threads.
     * Each worker independently finds and executes migrations.
     */
    private void runContinuous() {
        logger.info("Starting balancer in continuous mode with {} workers",
                   migrationExecutor instanceof java.util.concurrent.ThreadPoolExecutor
                       ? ((java.util.concurrent.ThreadPoolExecutor) migrationExecutor).getCorePoolSize()
                       : "unknown");

        running = true;

        // Initial metrics refresh
        refreshMetricsIfNeeded();

        // Submit worker tasks
        int numWorkers = migrationExecutor instanceof java.util.concurrent.ThreadPoolExecutor
            ? ((java.util.concurrent.ThreadPoolExecutor) migrationExecutor).getCorePoolSize()
            : 2;

        List<Future<?>> workerFutures = new ArrayList<>();
        for (int i = 0; i < numWorkers; i++) {
            final int workerId = i;
            Future<?> future = migrationExecutor.submit(() -> workerLoop(workerId));
            workerFutures.add(future);
        }

        // Print initial status report
        printStatusReport();

        // Start status reporting thread
        long reportIntervalMs = config.getStatusReportIntervalMs();
        long reportIntervalSeconds = reportIntervalMs / 1000;
        logger.info("Status reports will be printed every {} seconds", reportIntervalSeconds);

        statusReporter = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "status-reporter");
            t.setDaemon(true);
            return t;
        });
        ((java.util.concurrent.ScheduledExecutorService) statusReporter)
            .scheduleAtFixedRate(this::printStatusReport, reportIntervalMs, reportIntervalMs,
                               java.util.concurrent.TimeUnit.MILLISECONDS);

        // Wait for all workers to complete (they run until interrupted)
        for (Future<?> future : workerFutures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                logger.info("Worker interrupted, shutting down");
                Thread.currentThread().interrupt();
                break;
            } catch (ExecutionException e) {
                logger.error("Worker failed", e);
            }
        }
    }

    /**
     * Run balancer once and exit.
     */
    private void runOnce() {
        logger.info("Running balancer once");

        // Collect fresh metrics (collectShardMetrics internally calls refreshMetrics)
        List<ShardMetrics> allMetrics = metricsCollector.collectShardMetrics();

        if (allMetrics.isEmpty()) {
            logger.warn("No shard metrics available");
            return;
        }

        logShardMetrics(allMetrics);

        // Calculate weighted scores
        Map<String, Double> weightedScores = calculateWeightedScores(allMetrics);
        logWeightedScores(weightedScores);

        synchronized (balancingLock) {
            cachedMetrics = allMetrics;
            cachedScores = weightedScores;
        }

        // Select multiple migrations with shard exclusion
        List<PlannedMigration> plannedMigrations = selectMigrations(allMetrics, weightedScores);

        if (plannedMigrations.isEmpty()) {
            logger.info("No migrations needed - cluster is balanced");
            return;
        }

        logger.info("Selected {} migrations for parallel execution", plannedMigrations.size());
        for (PlannedMigration migration : plannedMigrations) {
            logger.info("  - {} -> {}: {}",
                       migration.getSourceShard(),
                       migration.getDestinationShard(),
                       migration.getChunk().getNs());
        }

        // Execute all migrations in parallel
        executeInParallel(plannedMigrations);

        // Log results
        int successful = (int) plannedMigrations.stream().filter(PlannedMigration::isSuccess).count();
        int failed = plannedMigrations.size() - successful;

        logger.info("Balancing complete: {} successful, {} failed", successful, failed);
    }

    /**
     * Worker loop that continuously finds and executes migrations.
     * Each worker independently:
     * 1. Refreshes metrics if needed
     * 2. Finds an available source/dest shard pair
     * 3. Executes the migration
     * 4. Invalidates chunk cache
     * 5. Sleeps and repeats
     */
    private void workerLoop(int workerId) {
        logger.info("Worker {} started", workerId);
        int consecutiveNoWork = 0;

        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                // Try to find and execute a migration
                PlannedMigration migration = findAndAcquireMigration(workerId);

                if (migration == null) {
                    consecutiveNoWork++;

                    // Exponential backoff when cluster is balanced (max 30 seconds)
                    long sleepMs = Math.min(1000L * consecutiveNoWork, 30000L);

                    if (consecutiveNoWork == 1) {
                        logger.debug("Worker {}: No migrations available", workerId);
                    } else if (consecutiveNoWork % 10 == 0) {
                        logger.info("Worker {}: Cluster appears balanced, sleeping {}s between checks",
                                   workerId, sleepMs / 1000);
                    }

                    Thread.sleep(sleepMs);
                    continue;
                }

                // Reset counter when work is found
                consecutiveNoWork = 0;

                // Execute the migration
                boolean success = executeMigration(migration, workerId);

                // Release the shards and namespace
                synchronized (balancingLock) {
                    inUseShards.remove(migration.getSourceShard());
                    inUseShards.remove(migration.getDestinationShard());
                    inUseNamespaces.remove(migration.getChunk().getNs());
                    balancingLock.notifyAll();
                }

                if (success) {
                    // Update chunk registry with the migration (no reload needed)
                    chunkRegistry.recordMigration(migration.getChunk().getNs(),
                                                 migration.getChunk(),
                                                 migration.getSourceShard(),
                                                 migration.getDestinationShard());

                    // Sleep between moves
                    rateLimiter.sleepBetweenMoves();
                }

            } catch (InterruptedException e) {
                logger.info("Worker {} interrupted", workerId);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Worker {} encountered error", workerId, e);
                // Continue running despite error
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        logger.info("Worker {} stopped", workerId);
    }

    /**
     * Find an available migration and mark the shards as in-use.
     * Returns null if no migration is available.
     */
    private PlannedMigration findAndAcquireMigration(int workerId) {
        synchronized (balancingLock) {
            // Refresh metrics if needed
            refreshMetricsIfNeeded();

            if (cachedMetrics.isEmpty()) {
                return null;
            }

            // Check rate limits (0 means unlimited)
            if (rateLimiter.getMaxChunksPerHour() > 0 &&
                rateLimiter.getChunksMovedThisHour() >= rateLimiter.getMaxChunksPerHour()) {
                logger.debug("Worker {}: Rate limit reached", workerId);
                return null;
            }

            // Find available shards (not in use)
            Set<String> availableShards = cachedMetrics.stream()
                .map(ShardMetrics::getShardId)
                .filter(shard -> !inUseShards.contains(shard))
                .collect(Collectors.toSet());

            if (availableShards.size() < 2) {
                logger.debug("Worker {}: Not enough available shards ({}/{})",
                           workerId, availableShards.size(), cachedMetrics.size());
                return null;
            }

            // Find best source shard from available shards
            String sourceShard = getBestShard(cachedScores, availableShards, true);
            if (sourceShard == null) {
                logger.debug("Worker {}: No source shard found", workerId);
                return null;
            }

            // Find best dest shard from available shards (excluding source)
            Set<String> destsWithoutSource = new HashSet<>(availableShards);
            destsWithoutSource.remove(sourceShard);
            String destShard = getBestShard(cachedScores, destsWithoutSource, false);
            if (destShard == null) {
                logger.debug("Worker {}: No dest shard found", workerId);
                return null;
            }

            logger.debug("Worker {}: Checking threshold for {} -> {}", workerId, sourceShard, destShard);

            // Check threshold
            if (!meetsThreshold(sourceShard, destShard, cachedMetrics)) {
                logger.info("Worker {}: Source {} and dest {} do not meet threshold",
                          workerId, sourceShard, destShard);
                return null;
            }

            // Get chunks for source shard
            List<Megachunk> sourceChunks = getChunksForShard(sourceShard);
            if (sourceChunks.isEmpty()) {
                logger.debug("Worker {}: No movable chunks on source shard {}", workerId, sourceShard);
                return null;
            }

            // Select a chunk
            ShardMetrics sourceMetrics = getMetricsForShard(cachedMetrics, sourceShard);
            Megachunk chunkToMove = chunkSelector.selectChunk(sourceMetrics, sourceChunks);

            if (chunkToMove == null) {
                logger.debug("Worker {}: Chunk selector returned null for shard {}", workerId, sourceShard);
                return null;
            }

            // Check if namespace is already in use (MongoDB allows only 1 migration per namespace)
            String namespace = chunkToMove.getNs();
            if (inUseNamespaces.contains(namespace)) {
                logger.debug("Worker {}: Namespace {} already in use", workerId, namespace);
                return null;
            }

            // Mark shards and namespace as in-use
            inUseShards.add(sourceShard);
            inUseShards.add(destShard);
            inUseNamespaces.add(namespace);

            // Create the migration
            PlannedMigration migration = new PlannedMigration(chunkToMove, sourceShard, destShard);

            logger.debug("Worker {}: Acquired migration {} -> {} ({})",
                       workerId, sourceShard, destShard, chunkToMove.getNs());

            return migration;
        }
    }

    /**
     * Execute a migration and record the result.
     */
    private boolean executeMigration(PlannedMigration migration, int workerId) {
        long startTime = System.currentTimeMillis();
        logger.debug("Worker {}: Executing migration {} -> {} ({})",
                   workerId,
                   migration.getSourceShard(),
                   migration.getDestinationShard(),
                   migration.getChunk().getNs());

        MoveChunkResult result = chunkMover.moveChunk(migration.getChunk(),
                                                      migration.getDestinationShard());

        migration.setSuccess(result.isSuccess());
        migration.setExecuted(true);

        long duration = System.currentTimeMillis() - startTime;

        if (result.isSuccess()) {
            rateLimiter.recordMove();
            statistics.recordSuccess(duration);
            logger.info("Worker {}: Migration completed successfully {} -> {} ({} ms)",
                      workerId,
                      migration.getSourceShard(),
                      migration.getDestinationShard(),
                      duration);
        } else {
            String errorMessage = result.getErrorMessage() != null
                ? result.getErrorMessage()
                : "Unknown error";
            statistics.recordFailure(migration.getSourceShard(),
                                    migration.getDestinationShard(),
                                    migration.getChunk().getNs(),
                                    errorMessage);
            logger.warn("Worker {}: Migration failed {} -> {} - {} - Chunk: {}[{} - {}]",
                      workerId,
                      migration.getSourceShard(),
                      migration.getDestinationShard(),
                      errorMessage,
                      migration.getChunk().getNs(),
                      migration.getChunk().getMin(),
                      migration.getChunk().getMax());

            if (result.getException() != null) {
                migration.setError(result.getException());
            }
        }

        return result.isSuccess();
    }

    /**
     * Refresh metrics if they're stale (older than metricsRefreshIntervalMs).
     * Should be called within synchronized(balancingLock).
     */
    private void refreshMetricsIfNeeded() {
        long now = System.currentTimeMillis();
        if (now - lastMetricsRefreshTime > config.getMetricsRefreshIntervalMs()) {
            logger.debug("Refreshing metrics (last refresh: {} ms ago)",
                        now - lastMetricsRefreshTime);

            // When namespace-focus is enabled, use fast namespace-based metrics
            // Otherwise use traditional dbStats approach
            if (config.isUseNamespaceFocus() && namespaceCollector != null) {
                // Refresh only the active namespaces (much faster)
                namespaceCollector.refreshNamespaces(activeNamespaces);

                // Generate shard metrics from namespace data
                cachedMetrics = namespaceCollector.generateShardMetrics();
                cachedScores = calculateWeightedScores(cachedMetrics);
                lastMetricsRefreshTime = now;

                logger.debug("Refreshed metrics from {} active namespaces", activeNamespaces.size());
            } else {
                // Traditional full dbStats collection
                cachedMetrics = metricsCollector.collectShardMetrics();
                cachedScores = calculateWeightedScores(cachedMetrics);
                lastMetricsRefreshTime = now;
            }

            logShardMetrics(cachedMetrics);
            logWeightedScores(cachedScores);
        }

        // Rescan namespaces periodically if namespace-focus is enabled
        if (config.isUseNamespaceFocus() && namespaceCollector != null) {
            if (now - lastNamespaceFullScanTime > config.getNamespaceFullScanIntervalMs()) {
                logger.info("Performing periodic full namespace scan (last scan: {} minutes ago)",
                    (now - lastNamespaceFullScanTime) / 60000);

                try {
                    List<String> imbalancedNamespaces = namespaceCollector.scanAllNamespaces();
                    lastNamespaceFullScanTime = now;

                    // Limit to maxActiveNamespaces
                    if (imbalancedNamespaces.size() > config.getMaxActiveNamespaces()) {
                        imbalancedNamespaces = imbalancedNamespaces.subList(0, config.getMaxActiveNamespaces());
                    }

                    // Update active namespaces
                    activeNamespaces = imbalancedNamespaces;
                    logger.info("Updated active namespaces to {} most imbalanced collections", activeNamespaces.size());
                } catch (Exception e) {
                    logger.error("Failed to rescan namespaces: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * Select multiple migrations following MongoDB's algorithm:
     * - Up to floor(numShards/2) migrations
     * - Each shard in at most 1 migration (source OR dest)
     * - Apply 3x threshold rule
     * - Skip jumbo chunks and disabled collections
     */
    private List<PlannedMigration> selectMigrations(List<ShardMetrics> allMetrics,
                                                     Map<String, Double> weightedScores) {
        List<PlannedMigration> migrations = new ArrayList<>();

        // Track namespaces already in use by selected migrations to avoid conflicts
        Set<String> usedNamespaces = new HashSet<>();

        // Available shards pool - removed as migrations are selected
        Set<String> availableShards = allMetrics.stream()
            .map(ShardMetrics::getShardId)
            .collect(Collectors.toSet());

        // Maximum concurrent migrations: floor(numShards / 2)
        int maxMigrations = Math.max(1, allMetrics.size() / 2);

        logger.info("Selecting up to {} migrations from {} available shards",
                   maxMigrations, availableShards.size());

        // Keep selecting source/dest pairs until no more valid pairs or limit reached
        while (migrations.size() < maxMigrations && availableShards.size() >= 2) {
            // Find best source shard from available shards
            String sourceShard = getBestShard(weightedScores, availableShards, true);
            if (sourceShard == null) {
                logger.debug("No more valid source shards");
                break;
            }

            // Find best dest shard from available shards (excluding source)
            Set<String> destsWithoutSource = new HashSet<>(availableShards);
            destsWithoutSource.remove(sourceShard);
            String destShard = getBestShard(weightedScores, destsWithoutSource, false);
            if (destShard == null) {
                logger.debug("No more valid destination shards");
                break;
            }

            // Check 3x threshold rule (MongoDB's imbalance threshold)
            if (!meetsThreshold(sourceShard, destShard, allMetrics)) {
                logger.info("Source {} and dest {} do not meet 3x threshold, stopping selection",
                          sourceShard, destShard);
                break;
            }

            // Check if source shard is draining (would be highest priority)
            boolean isDraining = com.mongodb.util.ChunkUtils.isShardDraining(shardClient, sourceShard);
            if (isDraining) {
                logger.warn("Source shard {} is draining - this balancer doesn't handle draining yet",
                          sourceShard);
            }

            // Select a chunk to move from source
            List<Megachunk> sourceChunks = getChunksForShard(sourceShard);
            if (sourceChunks.isEmpty()) {
                logger.info("No movable chunks on source shard {}, removing from pool", sourceShard);
                availableShards.remove(sourceShard);
                continue;
            }

            ShardMetrics sourceMetrics = getMetricsForShard(allMetrics, sourceShard);
            Megachunk chunkToMove = chunkSelector.selectChunk(sourceMetrics, sourceChunks);

            if (chunkToMove == null) {
                logger.info("Chunk selector returned null for shard {}", sourceShard);
                availableShards.remove(sourceShard);
                continue;
            }

            // Check if this namespace is already in use by another migration
            String namespace = chunkToMove.getNs();
            if (usedNamespaces.contains(namespace)) {
                logger.info("Namespace {} already in use by another migration, skipping source shard {}",
                          namespace, sourceShard);
                availableShards.remove(sourceShard);
                continue;
            }

            // Create the planned migration
            PlannedMigration migration = new PlannedMigration(chunkToMove, sourceShard, destShard);
            migrations.add(migration);
            usedNamespaces.add(namespace);

            logger.info("Selected migration #{}: {} -> {} (chunk: {})",
                       migrations.size(), sourceShard, destShard,
                       chunkToMove.getNs() + "[" + chunkToMove.getMin() + "]");

            // CRITICAL: Remove BOTH shards from available pool
            availableShards.remove(sourceShard);
            availableShards.remove(destShard);

            logger.debug("Removed {} and {} from available pool, {} shards remaining",
                        sourceShard, destShard, availableShards.size());
        }

        return migrations;
    }

    /**
     * Execute migrations in parallel using thread pool.
     * Respects rate limits across all migrations.
     */
    private void executeInParallel(List<PlannedMigration> migrations) {
        if (migrations.isEmpty()) {
            return;
        }

        // Check rate limits before starting
        int allowedMoves = Math.min(migrations.size(), rateLimiter.getMaxChunksPerHour() -
                                                       rateLimiter.getChunksMovedThisHour());
        if (allowedMoves <= 0) {
            logger.warn("Rate limit reached, waiting...");
            rateLimiter.waitForRateLimitReset();
            return;
        }

        List<PlannedMigration> migrationsToExecute = migrations.subList(0, Math.min(migrations.size(), allowedMoves));

        logger.info("Executing {} migrations in parallel (rate limit: {}/hr, used: {}/hr)",
                   migrationsToExecute.size(),
                   rateLimiter.getMaxChunksPerHour(),
                   rateLimiter.getChunksMovedThisHour());

        // Submit all migrations as parallel tasks
        List<Future<Boolean>> futures = new ArrayList<>();
        for (PlannedMigration migration : migrationsToExecute) {
            Future<Boolean> future = migrationExecutor.submit(() -> {
                MoveChunkResult result = chunkMover.moveChunk(migration.getChunk(),
                                                              migration.getDestinationShard());
                migration.setSuccess(result.isSuccess());
                migration.setExecuted(true);

                if (!result.isSuccess()) {
                    logger.error("Migration failed: {} -> {} - {}",
                               migration.getSourceShard(),
                               migration.getDestinationShard(),
                               result.getDescription());
                    if (result.getException() != null) {
                        migration.setError(result.getException());
                    }
                }

                return result.isSuccess();
            });
            futures.add(future);
        }

        // Wait for all to complete
        for (int i = 0; i < futures.size(); i++) {
            try {
                boolean success = futures.get(i).get();
                PlannedMigration migration = migrationsToExecute.get(i);

                if (success) {
                    rateLimiter.recordMove();
                    logger.info("Migration completed: {} -> {}",
                              migration.getSourceShard(),
                              migration.getDestinationShard());
                } else {
                    Exception error = migration.getError();
                    if (error != null) {
                        logger.warn("Migration failed: {} -> {}: {}",
                                  migration.getSourceShard(),
                                  migration.getDestinationShard(),
                                  error.getMessage());
                    } else {
                        logger.warn("Migration failed: {} -> {} (no error details)",
                                  migration.getSourceShard(),
                                  migration.getDestinationShard());
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Error waiting for migration to complete", e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        // Sleep between moves if configured (applies after all parallel moves complete)
        if (migrationsToExecute.stream().anyMatch(PlannedMigration::isSuccess)) {
            rateLimiter.sleepBetweenMoves();
        }
    }

    /**
     * Check if the imbalance between source and dest warrants a migration.
     *
     * For MongoDB 6.0+ with moveRange, chunk-based thresholds are obsolete.
     * Instead, we use an absolute threshold (configurable via --threshold):
     *   - Only balance if difference >= threshold (e.g., 100MB, 1GB)
     *   - Prevents unnecessary moves for differences below the threshold
     *   - Works well for both small test clusters and large production clusters
     */
    private boolean meetsThreshold(String sourceShard, String destShard, List<ShardMetrics> allMetrics) {
        ShardMetrics sourceMetrics = getMetricsForShard(allMetrics, sourceShard);
        ShardMetrics destMetrics = getMetricsForShard(allMetrics, destShard);

        if (sourceMetrics == null || destMetrics == null) {
            return false;
        }

        // Use data size instead of storage size (storage doesn't get released automatically)
        long sourceSize = sourceMetrics.getDataSize();
        long destSize = destMetrics.getDataSize();
        long difference = sourceSize - destSize;
        long thresholdBytes = config.getThresholdBytes();

        boolean meets = difference >= thresholdBytes;

        logger.debug("Threshold check: {} ({}) -> {} ({}), diff: {}, threshold: {}, meets: {}",
                    sourceShard, formatSize(sourceSize),
                    destShard, formatSize(destSize),
                    formatSize(difference),
                    formatSize(thresholdBytes),
                    meets);

        return meets;
    }

    /**
     * Get the best shard (highest or lowest score) from available shards.
     */
    private String getBestShard(Map<String, Double> weightedScores,
                               Set<String> availableShards,
                               boolean highest) {
        return availableShards.stream()
            .filter(weightedScores::containsKey)
            .max(highest ? Comparator.comparingDouble(weightedScores::get)
                        : Comparator.comparingDouble(weightedScores::get).reversed())
            .orElse(null);
    }

    /**
     * Calculate weighted scores by combining all strategy scores.
     */
    private Map<String, Double> calculateWeightedScores(List<ShardMetrics> allMetrics) {
        Map<String, Double> weightedScores = new HashMap<>();

        // Initialize all shards to 0.0
        for (ShardMetrics metrics : allMetrics) {
            weightedScores.put(metrics.getShardId(), 0.0);
        }

        double totalWeight = 0.0;
        for (BalancingStrategy strategy : strategies) {
            totalWeight += strategy.getWeight();
        }

        // Calculate weighted sum of all strategies
        for (BalancingStrategy strategy : strategies) {
            Map<String, Double> strategyScores = strategy.calculateShardScores(allMetrics);

            for (Map.Entry<String, Double> entry : strategyScores.entrySet()) {
                String shardId = entry.getKey();
                double score = entry.getValue();
                double weightedScore = score * strategy.getWeight();

                weightedScores.put(shardId, weightedScores.get(shardId) + weightedScore);
            }
        }

        // Normalize by total weight
        if (totalWeight > 0) {
            for (String shardId : weightedScores.keySet()) {
                weightedScores.put(shardId, weightedScores.get(shardId) / totalWeight);
            }
        }

        return weightedScores;
    }

    /**
     * Get movable chunks for a specific shard.
     * Uses the central ChunkRegistry for consistent, cached chunk tracking.
     */
    private List<Megachunk> getChunksForShard(String shardId) {
        return chunkRegistry.getChunksForShard(shardId);
    }

    /**
     * Get metrics for a specific shard.
     */
    private ShardMetrics getMetricsForShard(List<ShardMetrics> allMetrics, String shardId) {
        for (ShardMetrics metrics : allMetrics) {
            if (metrics.getShardId().equals(shardId)) {
                return metrics;
            }
        }
        return null;
    }

    /**
     * Get shard with maximum score.
     */
    private String getMaxShard(Map<String, Double> scores) {
        return scores.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse(null);
    }

    /**
     * Get shard with minimum score.
     */
    private String getMinShard(Map<String, Double> scores) {
        return scores.entrySet().stream()
            .min(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse(null);
    }

    /**
     * Log configuration settings.
     */
    private void logConfiguration() {
        logger.info("Configuration:");
        logger.info("  MongoDB URI: {}", config.getSourceClusterUri());
        logger.info("  Strategies: {}", Arrays.toString(config.getStrategies()));
        logger.info("  Chunk selector: {}", config.getChunkSelectionStrategy());
        logger.info("  Balancing threshold: {}", formatSize(config.getThresholdBytes()));
        logger.info("  Max chunks/hour: {}", config.getMaxChunksPerHour());
        logger.info("  Max chunks/day: {}", config.getMaxChunksPerDay());
        logger.info("  Sleep between moves: {} ms", config.getSleepBetweenMovesMs());
        logger.info("  Continuous mode: {}", config.isContinuousMode());
        logger.info("  Dry run: {}", config.isDryRun());
        logger.info("  Secondary throttle: {}", config.isUseSecondaryThrottle());
        logger.info("  Wait for delete: {}", config.isWaitForDelete());
    }

    /**
     * Log shard metrics.
     * Note: Chunk counts are de-emphasized as they're largely irrelevant for MongoDB 6.0+
     */
    private void logShardMetrics(List<ShardMetrics> allMetrics) {
        logger.info("Shard Metrics:");
        for (ShardMetrics metrics : allMetrics) {
            logger.info("  {}: storage={}, data={}",
                       metrics.getShardId(),
                       formatSize(metrics.getStorageSize()),
                       formatSize(metrics.getDataSize()));
        }
    }

    /**
     * Format size in human-readable format (MB or GB).
     */
    private String formatSize(long bytes) {
        double gb = bytes / (1024.0 * 1024.0 * 1024.0);
        double mb = bytes / (1024.0 * 1024.0);

        if (gb >= 1.0) {
            return String.format("%.2f GB", gb);
        } else if (mb >= 0.01) {
            return String.format("%.2f MB", mb);
        } else {
            return String.format("%d bytes", bytes);
        }
    }

    /**
     * Log weighted scores.
     */
    private void logWeightedScores(Map<String, Double> scores) {
        logger.info("Weighted Scores:");
        scores.entrySet().stream()
            .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
            .forEach(entry ->
                logger.info("  {}: {}", entry.getKey(), String.format("%.3f", entry.getValue()))
            );
    }

    /**
     * Print comprehensive status report for monitoring.
     */
    private void printStatusReport() {
        try {
            logger.info("=".repeat(80));
            logger.info("BALANCER STATUS REPORT");
            logger.info("=".repeat(80));

            // Uptime and activity
            logger.info("Uptime: {} minutes", statistics.getUptimeMinutes());
            long minsSinceLastSuccess = statistics.getMinutesSinceLastSuccess();
            if (minsSinceLastSuccess >= 0) {
                logger.info("Last successful migration: {} minutes ago", minsSinceLastSuccess);
            } else {
                logger.info("Last successful migration: Never");
            }

            // Migration statistics
            logger.info("Migrations - Total: {} successful, {} failed",
                       statistics.getSuccessCount(),
                       statistics.getFailureCount());
            logger.info("Migrations - Last hour: {}, Last 24 hours: {}",
                       statistics.getHourlyCount(),
                       statistics.getDailyCount());
            logger.info("Average migration duration: {} ms", statistics.getAverageDurationMs());

            // Rate limiting
            logger.info("Rate limits - {}/{} per hour, {}/{} per day",
                       rateLimiter.getChunksMovedThisHour(),
                       rateLimiter.getMaxChunksPerHour(),
                       rateLimiter.getChunksMovedToday(),
                       rateLimiter.getMaxChunksPerDay());

            // Recent failures
            List<com.mongodb.balancer.stats.BalancerStatistics.FailureRecord> recentFailures = statistics.getRecentFailures();
            if (!recentFailures.isEmpty()) {
                logger.info("Recent failures (last {}):", recentFailures.size());
                for (com.mongodb.balancer.stats.BalancerStatistics.FailureRecord failure : recentFailures) {
                    logger.info("  [{}] {} -> {} ({}): {}",
                               failure.getFormattedTimestamp(),
                               failure.getSourceShard(),
                               failure.getDestShard(),
                               failure.getNamespace(),
                               failure.getErrorMessage());
                }
            }

            // Current shard distribution
            synchronized (balancingLock) {
                // Always refresh metrics for status report to show current state
                // collectShardMetrics internally calls refreshMetrics
                cachedMetrics = metricsCollector.collectShardMetrics();
                cachedScores = calculateWeightedScores(cachedMetrics);
                lastMetricsRefreshTime = System.currentTimeMillis();

                if (!cachedMetrics.isEmpty()) {
                    logger.info("Current shard distribution:");
                    for (ShardMetrics metrics : cachedMetrics) {
                        logger.info("  {}: storage={}, data={}, score={}",
                                   metrics.getShardId(),
                                   formatBytes(metrics.getStorageSize()),
                                   formatBytes(metrics.getDataSize()),
                                   String.format("%.3f", cachedScores.getOrDefault(metrics.getShardId(), 0.0)));
                    }

                    // Check if balanced
                    Set<String> allShards = cachedMetrics.stream()
                        .map(ShardMetrics::getShardId).collect(Collectors.toSet());
                    String sourceShard = getBestShard(cachedScores, allShards, true);
                    String destShard = getBestShard(cachedScores, allShards.stream()
                        .filter(s -> !s.equals(sourceShard)).collect(Collectors.toSet()), false);

                    if (sourceShard != null && destShard != null) {
                        boolean balanced = !meetsThreshold(sourceShard, destShard, cachedMetrics);
                        logger.info("Balance status: {}", balanced ? "BALANCED (no migrations meet threshold)" :
                                   "UNBALANCED (migrations available)");
                    }
                }
            }

            logger.info("=".repeat(80));

        } catch (Exception e) {
            logger.error("Error printing status report", e);
        }
    }

    /**
     * Format bytes in human-readable format.
     */
    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp - 1) + "";
        return String.format("%.2f %sB", bytes / Math.pow(1024, exp), pre);
    }

    /**
     * Set the logging level dynamically.
     */
    private void setLogLevel(String level) {
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        root.setLevel(ch.qos.logback.classic.Level.toLevel(level));
    }

    /**
     * Cleanup resources.
     */
    private void cleanup() {
        logger.info("Cleaning up resources");

        // Stop workers
        running = false;

        // Shutdown status reporter
        if (statusReporter != null) {
            logger.info("Shutting down status reporter");
            statusReporter.shutdown();
            try {
                if (!statusReporter.awaitTermination(10, TimeUnit.SECONDS)) {
                    statusReporter.shutdownNow();
                }
            } catch (InterruptedException e) {
                statusReporter.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Shutdown migration executor
        if (migrationExecutor != null) {
            logger.info("Shutting down migration executor");
            migrationExecutor.shutdown();
            try {
                if (!migrationExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                    logger.warn("Migration executor did not terminate in time, forcing shutdown");
                    migrationExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for executor shutdown");
                migrationExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // ShardClient doesn't have an explicit close() method
        // Cleanup is handled automatically
    }
}
