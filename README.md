# MongoDB Strategy-Based Balancer

A flexible, strategy-based MongoDB shard balancer framework that allows you to customize balancing behavior based on your specific needs.

## Overview

The built-in MongoDB balancer primarily focuses on data size and has fixed thresholds for balancing decisions. This custom balancer framework allows you to:

- **Combine multiple balancing strategies** with configurable weights (disk space, data size, chunk count)
- **Prioritize disk space balancing** to prevent running out of disk on specific shards
- **Use moveRange** (MongoDB 6.0+) instead of moveChunk for improved performance
- **Control the rate** of chunk moves to avoid overwhelming your cluster
- **Customize chunk selection** logic (random, largest, smallest)

## Key Features

- **Strategy Pattern Architecture**: Plug in different balancing strategies and combine them with weighted scoring
- **Version Detection**: Automatically uses `moveRange` for MongoDB 6.0+ and falls back to `moveChunk` for older versions
- **Rate Limiting**: Configure max chunks per hour/day and sleep intervals between moves
- **Dry Run Mode**: Test your configuration without actually moving any chunks
- **Continuous or One-Shot**: Run continuously in a loop or execute once and exit
- **Built on mongo-util-core**: Leverages proven infrastructure from the mongo-util project

## Architecture

### Core Components

1. **BalancingStrategy**: Interface for strategies that score shards (0.0 = underutilized, 1.0 = overloaded)
   - `DataSizeBalancingStrategy`: Scores based on logical data size (recommended)
   - `ChunkCountBalancingStrategy`: Scores based on number of chunks

2. **ChunkSelectionStrategy**: Interface for selecting which chunk to move
   - `RandomChunkSelector`: Randomly select a chunk
   - `LargestChunkSelector`: Select the largest chunk (placeholder)
   - `SmallestChunkSelector`: Select the smallest chunk (placeholder)

3. **MetricsCollector**: Gathers shard metrics using `dbStats` command
4. **ChunkMover**: Handles chunk movement with auto-split on ChunkTooBig errors
5. **RateLimiter**: Controls the rate of chunk moves

### Weighted Scoring

Multiple strategies are combined using weighted scoring:

```
finalScore = (score1 × weight1 + score2 × weight2 + ...) / totalWeight
```

Example with `dataSize:1.0,chunkCount:0.3`:
- Shard0: dataSize=0.9, chunkCount=0.7 → weighted=(0.9×1.0)+(0.7×0.3)/1.3 = **0.85**
- Shard5: dataSize=0.3, chunkCount=0.8 → weighted=(0.3×1.0)+(0.8×0.3)/1.3 = **0.42**

The balancer moves chunks from Shard0 (highest score) to Shard5 (lowest score).

**Note**: The `diskSpace` strategy has been removed because storage size doesn't get released automatically after data is moved, making it unreliable for balancing decisions.

## Building

```bash
mvn clean package
```

This creates:
- `target/mongodb-strategy-balancer-1.0.0.jar` - Core JAR
- `bin/mongodb-strategy-balancer.jar` - Uber JAR with all dependencies

## Configuration

Create a `balancer.properties` file (see `balancer.properties` for full example):

```properties
# MongoDB connection
sourceClusterUri=mongodb://localhost:27017

# Balancing strategies with weights
strategies=dataSize:1.0,chunkCount:0.1

# Chunk selection
chunkSelectionStrategy=random

# Rate limiting
maxChunksPerHour=20
maxChunksPerDay=400
sleepBetweenMovesMs=500

# Balancing loop
continuousMode=true

# Options
useSecondaryThrottle=true
waitForDelete=true
disableBuiltInBalancer=true
dryRun=false
```

## Usage

### Run with default config file (balancer.properties)

```bash
java -jar bin/mongodb-strategy-balancer.jar
```

### Run with custom config file

```bash
java -jar bin/mongodb-strategy-balancer.jar --config /path/to/custom.properties
```

### Dry run mode (test without moving chunks)

```bash
# Set in properties file
dryRun=true

# Or run once and exit
continuousMode=false
```

## Use Cases

### 1. Data Size Balancing

**Problem**: Need to balance data evenly across shards while considering chunk distribution.

**Solution**: Use data size as primary metric with chunk count as secondary:

```properties
strategies=dataSize:1.0,chunkCount:0.3
chunkSelectionStrategy=random
maxChunksPerHour=30
continuousMode=true
```

This configuration:
- Primarily balances by data size (1.0) to ensure even data distribution
- Considers chunk count (0.3) as a secondary factor to prevent chunk hotspots
- Moves chunks from data-heavy shards to lighter shards

### 2. Shard Removal

**Problem**: Need to drain specific shards before decommissioning.

**Solution**: Use namespace filters to target specific shards (configure via BaseConfiguration):

```properties
# This would require extending the framework to support source/dest shard filtering
# Or use the existing ShardRemovalBalancer in mongo-util for this use case
```

### 3. Chunk Distribution Evening

**Problem**: Chunks are unevenly distributed across shards.

**Solution**: Prioritize chunk count:

```properties
strategies=chunkCount:1.0,dataSize:0.3
maxChunksPerHour=50
```

## Extending the Framework

### Add a Custom Balancing Strategy

1. Implement the `BalancingStrategy` interface:

```java
public class MyCustomStrategy implements BalancingStrategy {
    private double weight;

    @Override
    public Map<String, Double> calculateShardScores(List<ShardMetrics> allMetrics) {
        // Your scoring logic here
        // Return scores from 0.0 (underutilized) to 1.0 (overloaded)
    }

    @Override
    public double getWeight() { return weight; }

    @Override
    public void setWeight(double weight) { this.weight = weight; }

    @Override
    public String getName() { return "myCustom"; }
}
```

2. Register it in `StrategyBasedBalancer.createStrategy()`:

```java
case "mycustom":
    return new MyCustomStrategy(weight);
```

3. Use it in your config:

```properties
strategies=myCustom:1.0,dataSize:0.5
```

### Add a Custom Chunk Selection Strategy

1. Implement the `ChunkSelectionStrategy` interface:

```java
public class MyChunkSelector implements ChunkSelectionStrategy {
    @Override
    public Megachunk selectChunk(ShardMetrics sourceMetrics, List<Megachunk> availableChunks) {
        // Your selection logic here
    }

    @Override
    public String getName() { return "mySelector"; }
}
```

2. Register it in `StrategyBasedBalancer.loadChunkSelector()`.

## Comparison with Built-in Balancer

| Feature | Built-in Balancer | Strategy-Based Balancer |
|---------|------------------|------------------------|
| Balancing Criteria | Data size only | Multiple strategies (disk, data, chunks) |
| Customization | Fixed thresholds | Fully configurable weights |
| moveRange Support | Yes (8.0+) | Yes (6.0+) with auto-detection |
| Rate Limiting | Basic | Configurable (hourly/daily limits) |
| Chunk Selection | Internal algorithm | Pluggable strategies |
| Dry Run | No | Yes |
| Extensibility | None | Plugin architecture |

## Limitations

1. **Chunk size selection**: Largest/smallest chunk selectors are placeholders (requires expensive dataSize calls)
2. **Single namespace at a time**: Currently processes all namespaces together
3. **No time windows**: Unlike ShardRemovalBalancer, doesn't support daily time windows
4. **Metrics collection overhead**: dbStats calls can be slow on large databases

## Troubleshooting

### "ChunkTooBig" errors

The ChunkMover automatically handles this by splitting chunks with `splitFind` and continuing. If you see frequent ChunkTooBig errors:

1. Use `smallest` chunk selection strategy
2. Reduce `maxChunksPerHour` to avoid overwhelming the cluster
3. Consider pre-splitting large chunks manually

### Balancer not moving chunks

Check:
1. Are the weighted scores actually different between shards? (Check logs)
2. Is `dryRun=true` set?
3. Have you hit rate limits? (Check `maxChunksPerHour`/`maxChunksPerDay`)
4. Is the built-in balancer disabled?

### Slow metrics collection

`dbStats` on large databases can be slow. Consider:
1. Increasing `metricsRefreshIntervalMs` to collect less frequently
2. Adding filters to exclude certain databases (via `includeDatabases`)

## Monitoring

The balancer logs detailed information:
- Shard metrics (storage, data size, chunk count)
- Strategy scores for each shard
- Weighted final scores
- Chunk move attempts and results
- Rate limiter status

Logs are output using SLF4J/Logback. Configure logging in `logback.xml`.

## License

Apache License 2.0 (same as mongo-util-core)



