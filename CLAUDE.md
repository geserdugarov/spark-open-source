# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

Apache Spark 4.2.0-SNAPSHOT — a unified analytics engine for large-scale data processing. Written primarily in Scala 2.13, with Java, Python (PySpark), and R (deprecated) APIs. Requires Java 17+ and Maven 3.9.12+.

## Build Commands

```bash
# Set Maven memory (needed for builds)
export MAVEN_OPTS="-Xss64m -Xmx4g -Xms4g -XX:ReservedCodeCacheSize=128m"

# Basic build (skipping tests)
./build/mvn -DskipTests clean package

# Build with Hive support (needed for many SQL tests)
./build/mvn -DskipTests -Phive -Phive-thriftserver clean package

# Build a single module (faster iteration)
./build/mvn -DskipTests -pl :spark-catalyst_2.13 package

# SBT alternative (faster incremental builds)
./build/sbt package
```

Maven profiles: `-Pyarn`, `-Phive`, `-Phive-thriftserver`, `-Pkubernetes`, `-Pdocker-integration-tests`, `-Phadoop-cloud`.

## Running Tests

```bash
# Full test suite
./dev/run-tests

# Single Maven module
./build/mvn test -pl :spark-core_2.13
./build/mvn test -pl :spark-catalyst_2.13
./build/mvn test -pl :spark-sql_2.13

# Single Scala/Java test class
./build/mvn test -pl :spark-catalyst_2.13 -Dtest=none -DwildcardSuites=org.apache.spark.sql.catalyst.optimizer.ConstantFoldingSuite

# Single test method (ScalaTest with -z for substring match)
./build/mvn test -pl :spark-catalyst_2.13 -Dtest=none -DwildcardSuites=org.apache.spark.sql.catalyst.optimizer.ConstantFoldingSuite -Dsuites="*ConstantFoldingSuite" -- -z "test name substring"

# SBT single test
./build/sbt "catalyst/testOnly *ConstantFoldingSuite"

# PySpark tests (specific module)
./python/run-tests --python-executables=python3 --modules=pyspark-sql

# PySpark tests (specific test file)
python3 -m pytest python/pyspark/tests/test_rdd.py

# Include/exclude test tags
./build/mvn test -Dtest.include.tags=TAG -Dtest.exclude.tags=TAG
```

## Linting and Formatting

```bash
./dev/lint-scala        # Scalastyle checks
./dev/lint-java         # Checkstyle checks
./dev/lint-python       # Python linting (black, ruff, mypy, flake8)
./dev/check-license     # Apache RAT license header checks
```

Python-specific lint options: `./dev/lint-python --black`, `--ruff`, `--mypy`, `--compile`.

Scala max line length: 100 characters (scalastyle-config.xml). Spark Connect Scala code uses scalafmt (config: `dev/.scalafmt.conf`).

## Project Architecture

### Module Structure

- **`core/`** — Spark Core: RDD API, task scheduling, shuffle, block manager, memory management
- **`sql/api/`** — Public SQL/DataFrame API interfaces
- **`sql/catalyst/`** — Query optimizer: parsing, analysis, logical planning, optimization rules, physical planning
- **`sql/core/`** — SQL execution engine: data sources, physical operators, Adaptive Query Execution (AQE)
- **`sql/hive/`** — Hive metastore integration and HiveQL support
- **`sql/connect/`** — Spark Connect: client-server architecture for remote Spark sessions via gRPC
  - `connect/server/` — gRPC server that translates Connect protocol to Spark operations
  - `connect/client/jvm/` — JVM client library
  - `connect/client/jdbc/` — JDBC thin client
  - `connect/common/` — Shared proto definitions
- **`sql/pipelines/`** — Declarative pipelines (pipeline DSL)
- **`streaming/`** — DStream-based streaming (legacy); Structured Streaming lives in `sql/core`
- **`mllib/`** — Distributed ML algorithms (DataFrame-based API)
- **`graphx/`** — Graph computation engine
- **`connector/`** — Data source connectors (Kafka, Avro, Protobuf)
- **`resource-managers/`** — YARN and Kubernetes cluster managers
- **`common/`** — Shared libraries: networking, unsafe memory, sketch, variant type, kvstore
- **`launcher/`** — Programmatic Spark submission API
- **`repl/`** — Interactive shell (Scala, Python)
- **`assembly/`** — Fat JAR packaging
- **`examples/`** — Example programs

### Catalyst Optimizer Pipeline

The query optimizer in `sql/catalyst/` processes queries through these phases:
1. **Parser** (`parser/`) — SQL/DataFrame → Unresolved Logical Plan
2. **Analyzer** (`analysis/`) — Resolve references, types → Resolved Logical Plan
3. **Optimizer** (`optimizer/`) — Rule-based transformations (constant folding, predicate pushdown, etc.) → Optimized Logical Plan
4. **Planner** (`planning/`) — Convert logical plan to physical plan with `SparkStrategy` implementations

Key abstractions: `TreeNode` (immutable tree base), `LogicalPlan`, `SparkPlan` (physical), `Expression`, `Rule[T]` (tree transformation), `Catalog`.

### Source Layout Per Module

```
<module>/src/main/scala/org/apache/spark/...   # Scala sources
<module>/src/main/java/org/apache/spark/...    # Java sources
<module>/src/test/scala/org/apache/spark/...   # Scala tests
<module>/src/main/resources/                    # Config/resource files
```

PySpark sources: `python/pyspark/` with tests in `python/pyspark/tests/` and per-subpackage `tests/` dirs.

### Key Entry Points

- `SparkSession` (`sql/core`) — Main entry point for DataFrame/SQL operations
- `SparkContext` (`core`) — Low-level RDD entry point
- `SparkSubmit` (`core`) — Application submission
- `SparkConnectService` (`sql/connect/server`) — Spark Connect gRPC server

## Code Conventions

- Scala style: follow `scalastyle-config.xml` rules; suppress locally with `// scalastyle:off <rule>` / `// scalastyle:on`
- Java style: follow `dev/checkstyle.xml`
- All source files require Apache 2.0 license headers
- Maven artifact names use `_2.13` suffix (Scala binary version)
- Module Maven artifact IDs: `spark-<module>_2.13` (e.g., `spark-core_2.13`, `spark-catalyst_2.13`, `spark-sql_2.13`)
