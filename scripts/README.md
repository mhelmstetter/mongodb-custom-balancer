# MongoDB Strategy Balancer - Command Installation Scripts

This directory contains scripts for installing mongodb-strategy-balancer commands as standalone executables in your system PATH.

## Installation

Run the installation script from the mongodb-strategy-balancer project root:

```bash
./scripts/install-commands.sh
```

This will:
1. Check for Java 17+ installation
2. Build the project if needed (mvn clean package)
3. Install command wrappers to `~/.local/bin`
4. Verify the installation

## Installed Commands

### balancer

Strategy-based MongoDB shard balancer that uses configurable strategies to determine optimal chunk movements across shards.

**Usage:**
```bash
balancer [-c <config-file>] [options]
```

**Options:**
- `-c, --config`: Configuration file path (default: balancer.properties)
- `-h, --help`: Show help message
- `-V, --version`: Show version information

**Examples:**
```bash
# Run with default config (balancer.properties in current directory)
balancer

# Run with specific config file
balancer -c /path/to/config.properties

# Run with custom config
balancer -c ~/my-balancer-config.properties

# Show help
balancer --help
```

## Adding New Commands

To add a new command to the installer:

1. **Create the main class** in `src/main/java/com/mongodb/<package>/`
   - Add picocli annotations for command-line parsing
   - Implement `Callable<Integer>` interface
   - Add a `main()` method

2. **Update install-commands.sh** by adding a new `create_command` call:

```bash
create_command "your-command-name" \
    "com.mongodb.package.YourMainClass" \
    "Description of your command"
```

3. **Update the installation output** in the script to include help text for the new command

4. **Rebuild and reinstall:**
```bash
mvn clean package -DskipTests
./scripts/install-commands.sh
```

### Example Template

```bash
# In install-commands.sh, add after the existing create_command calls:

create_command "balancer-validator" \
    "com.mongodb.balancer.validation.ValidatorMain" \
    "Validate balancer configuration and cluster state"
```

Then update the "Available Commands" section at the end of the script to document the new command.

## Updating

To update installed commands after code changes:

1. Make your code changes
2. Build the project: `mvn clean package -DskipTests`
3. Re-run the installer: `./scripts/install-commands.sh`

The installer will detect existing installations and update them.

## Uninstalling

To remove installed commands:

```bash
rm ~/.local/bin/balancer
# Add more commands here as they are added
```

## PATH Configuration

The installer checks if `~/.local/bin` is in your PATH. If not, add it to your shell profile:

**For zsh (macOS default):**
```bash
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

**For bash:**
```bash
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

## Troubleshooting

### "java command not found"
Install Java 17 or later:
- **macOS**: `brew install openjdk@17`
- **Ubuntu**: `sudo apt install openjdk-17-jdk`

### "JAR file not found"
The installer will automatically build the project. If this fails:
```bash
cd /path/to/mongodb-strategy-balancer
mvn clean package -DskipTests
```

### Command not found after installation
Check if `~/.local/bin` is in your PATH:
```bash
echo $PATH | grep -q "$HOME/.local/bin" && echo "In PATH" || echo "Not in PATH"
```

If not in PATH, see "PATH Configuration" above.

## How It Works

The installer:
1. Creates wrapper scripts in `~/.local/bin` (e.g., `balancer`)
2. Each wrapper points to the mongodb-strategy-balancer.jar in your project directory
3. When you run a command, it executes: `java -cp mongodb-strategy-balancer.jar com.mongodb.balancer.StrategyBasedBalancer "$@"`
4. The JAR stays in your project directory - no copying or duplication

This approach:
- Avoids JAR duplication
- Updates automatically when you rebuild
- Allows easy development and testing
- Provides clean command-line interface

## Configuration

The balancer reads its configuration from a properties file. See `balancer.properties` in the project root for an example configuration with:
- MongoDB connection URI
- Strategy weights
- Rate limiting settings
- Chunk selection preferences
- And more...
