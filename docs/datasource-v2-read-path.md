# Apache Spark DataSourceV2 Read Path

**Spark Version:** 4.2.0-SNAPSHOT

This document provides a comprehensive developer reference for the DataSourceV2 read-side architecture in Apache Spark. It covers the full chain from `TableProvider` to physical execution, including all pushdown optimization interfaces, streaming reads, catalog integration, and concrete examples from built-in sources.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Key Interfaces](#2-key-interfaces)
3. [Pushdown Optimizations](#3-pushdown-optimizations)
4. [Streaming Reads](#4-streaming-reads)
5. [Catalog Integration](#5-catalog-integration)
6. [Physical Execution](#6-physical-execution)
7. [File-Based V2 Sources](#7-file-based-v2-sources)
8. [Examples](#8-examples)
9. [Implementing a New V2 Data Source](#9-implementing-a-new-v2-data-source)

---

## 1. Overview

The DataSourceV2 (DSv2) read path defines a layered architecture where each interface represents a distinct phase of query planning and execution. The flow proceeds as follows:

```
TableProvider          -- Discovers and returns a Table
    |
    v
Table (+ SupportsRead) -- Describes schema, capabilities, creates ScanBuilder
    |
    v
ScanBuilder            -- Receives pushdowns (filters, columns, aggregates, limits)
    |                     Mix-in: SupportsPushDownFilters, SupportsPushDownRequiredColumns, etc.
    v
Scan                   -- Logical scan description; branches to Batch, MicroBatchStream, or ContinuousStream
    |
    v
Batch                  -- Plans physical InputPartitions and creates PartitionReaderFactory
    |
    v
InputPartition[]       -- Serializable partition descriptors sent to executors
    |
    v
PartitionReaderFactory -- Creates PartitionReader on each executor
    |
    v
PartitionReader<T>     -- Reads records (InternalRow or ColumnarBatch) on executors
```

**Key design principles:**

- **Separation of concerns:** Logical (Scan) and physical (Batch/InputPartition/PartitionReader) representations are distinct.
- **Pushdown before build:** The `ScanBuilder` collects pushdowns (filter, column pruning, aggregation, limit) *before* `build()` is called, so the resulting `Scan` reflects optimized state.
- **Serialization boundary:** `InputPartition` and `PartitionReaderFactory` must be `Serializable` -- they are shipped to executors. `PartitionReader` does not need to be serializable.
- **Columnar support:** The framework natively supports both row-based (`InternalRow`) and columnar (`ColumnarBatch`) reads via `PartitionReaderFactory.supportColumnarReads()`.

### Pushdown Order

As documented in `ScanBuilder`, the pushdown order is:

```
sample -> filter -> aggregate -> limit/top-n(sort + limit) -> offset -> column pruning
```

### Source Files

| Interface | Source Path |
|-----------|------------|
| `TableProvider` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/TableProvider.java` |
| `Table` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/Table.java` |
| `SupportsRead` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/SupportsRead.java` |
| `ScanBuilder` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/read/ScanBuilder.java` |
| `Scan` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/read/Scan.java` |
| `Batch` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/read/Batch.java` |
| `InputPartition` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/read/InputPartition.java` |
| `PartitionReaderFactory` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/read/PartitionReaderFactory.java` |
| `PartitionReader` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/read/PartitionReader.java` |

---

## 2. Key Interfaces

### 2.1 TableProvider

**Package:** `org.apache.spark.sql.connector.catalog`

The entry point for V2 data sources that do not have a real catalog. Implementations must have a public no-arg constructor. `TableProvider` can only apply data operations to existing tables (read, append, delete, overwrite) -- it does not support DDL operations like create/drop.

```java
@Evolving
public interface TableProvider {

  /**
   * Infer the schema of the table identified by the given options.
   */
  StructType inferSchema(CaseInsensitiveStringMap options);

  /**
   * Infer the partitioning of the table. Returns empty array by default.
   */
  default Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
    return new Transform[0];
  }

  /**
   * Return a Table instance with the specified schema, partitioning, and properties.
   * The returned table should report the same schema and partitioning as specified,
   * or Spark may fail the operation.
   */
  Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties);

  /**
   * Returns true if the source can accept external table metadata (user-specified
   * schema from DataFrameReader, or schema stored in Spark catalog). Default: false.
   */
  default boolean supportsExternalMetadata() {
    return false;
  }
}
```

### 2.2 Table

**Package:** `org.apache.spark.sql.connector.catalog`

Represents a logical structured data set. A `Table` by itself is just metadata; it gains read/write capabilities by mixing in `SupportsRead` and/or `SupportsWrite`.

```java
@Evolving
public interface Table {

  /** A name to identify this table (e.g., database.table or file path). */
  String name();

  /** Optional table ID for reliably checking identity. Returns null if unsupported. */
  default String id() { return null; }

  /**
   * Returns the columns of this table.
   * @deprecated Override columns() instead of schema().
   */
  @Deprecated(since = "3.4.0")
  default StructType schema() { ... }

  /** Returns the columns of this table as Column[]. */
  default Column[] columns() {
    return CatalogV2Util.structTypeToV2Columns(schema());
  }

  /** Physical partitioning of the table. Default: empty array. */
  default Transform[] partitioning() { return new Transform[0]; }

  /** Table properties (string key-value map). Default: empty. */
  default Map<String, String> properties() { return Collections.emptyMap(); }

  /** The set of capabilities for this table. */
  Set<TableCapability> capabilities();

  /** Table constraints. Default: empty. */
  default Constraint[] constraints() { return new Constraint[0]; }

  /** Table version if versioning is supported. Default: null. */
  default String version() { return null; }
}
```

### 2.3 TableCapability

The `TableCapability` enum signals what operations a table supports:

```java
public enum TableCapability {
  BATCH_READ,           // Batch read support
  MICRO_BATCH_READ,     // Micro-batch streaming read
  CONTINUOUS_READ,      // Continuous streaming read
  BATCH_WRITE,          // Batch write (append)
  STREAMING_WRITE,      // Streaming write
  TRUNCATE,             // Truncate support
  OVERWRITE_BY_FILTER,  // Overwrite with filter
  OVERWRITE_DYNAMIC,    // Dynamic partition overwrite
  ACCEPT_ANY_SCHEMA,    // Accept arbitrary input schema
  AUTOMATIC_SCHEMA_EVOLUTION, // Alter schema as part of operation
  V1_BATCH_WRITE        // V1 InsertableRelation write
}
```

A table that reports `BATCH_READ` must implement `Scan.toBatch()`. Similarly, `MICRO_BATCH_READ` requires `Scan.toMicroBatchStream()`, and `CONTINUOUS_READ` requires `Scan.toContinuousStream()`.

### 2.4 SupportsRead

**Package:** `org.apache.spark.sql.connector.catalog`

A mix-in for `Table` that makes it readable. This is the bridge between the table metadata layer and the scan/read layer.

```java
@Evolving
public interface SupportsRead extends Table {

  /**
   * Returns a ScanBuilder for configuring a data source scan.
   * Called once per query planning phase.
   */
  ScanBuilder newScanBuilder(CaseInsensitiveStringMap options);
}
```

### 2.5 ScanBuilder

**Package:** `org.apache.spark.sql.connector.read`

The builder that collects pushdown configurations before creating a `Scan`. Implementations mix in `SupportsPushDownXYZ` interfaces to receive pushdowns.

```java
@Evolving
public interface ScanBuilder {
  /**
   * Build the Scan after all pushdowns have been applied.
   * The returned Scan should reflect the pushed state.
   */
  Scan build();
}
```

### 2.6 Scan

**Package:** `org.apache.spark.sql.connector.read`

A logical representation of a data source scan, shared between batch, micro-batch, and continuous modes.

```java
@Evolving
public interface Scan {

  /** The actual schema of this scan (may differ from table schema due to pruning). */
  StructType readSchema();

  /** Human-readable description for EXPLAIN output. */
  default String description() { return this.getClass().toString(); }

  /** Physical batch representation. Required if table reports BATCH_READ. */
  default Batch toBatch() { throw ... }

  /** Physical micro-batch representation. Required if table reports MICRO_BATCH_READ. */
  default MicroBatchStream toMicroBatchStream(String checkpointLocation) { throw ... }

  /** Physical continuous representation. Required if table reports CONTINUOUS_READ. */
  default ContinuousStream toContinuousStream(String checkpointLocation) { throw ... }

  /** Custom metrics reported by this scan. */
  default CustomMetric[] supportedCustomMetrics() { return new CustomMetric[]{}; }

  /** Driver-side-only metrics. */
  default CustomTaskMetric[] reportDriverMetrics() { return new CustomTaskMetric[]{}; }

  /** Controls how columnar support is determined per partition. */
  enum ColumnarSupportMode { PARTITION_DEFINED, SUPPORTED, UNSUPPORTED }

  default ColumnarSupportMode columnarSupportMode() {
    return ColumnarSupportMode.PARTITION_DEFINED;
  }
}
```

### 2.7 Batch

**Package:** `org.apache.spark.sql.connector.read`

The physical representation of a batch scan. Plans partitions and creates the reader factory.

```java
@Evolving
public interface Batch {

  /**
   * Returns input partitions. Each InputPartition becomes one Spark task.
   * Called only once per scan to launch one Spark job.
   */
  InputPartition[] planInputPartitions();

  /**
   * Returns a factory to create a PartitionReader for each InputPartition.
   */
  PartitionReaderFactory createReaderFactory();
}
```

### 2.8 InputPartition

**Package:** `org.apache.spark.sql.connector.read`

A **serializable** descriptor for a data split. This object is serialized and shipped to executors. The actual reading happens in `PartitionReader`, which is created on the executor side.

```java
@Evolving
public interface InputPartition extends Serializable {

  /**
   * Preferred host locations for data locality.
   * Spark does not guarantee scheduling on these hosts.
   * Default: empty array (no preference).
   */
  default String[] preferredLocations() {
    return new String[0];
  }
}
```

### 2.9 PartitionReaderFactory

**Package:** `org.apache.spark.sql.connector.read`

A **serializable** factory that creates `PartitionReader` instances on executors.

```java
@Evolving
public interface PartitionReaderFactory extends Serializable {

  /** Creates a row-based reader for the given partition. */
  PartitionReader<InternalRow> createReader(InputPartition partition);

  /** Creates a columnar reader. Override if supportColumnarReads returns true. */
  default PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    throw new SparkUnsupportedOperationException(...);
  }

  /**
   * Whether the given partition should be read in columnar mode.
   * If true, createColumnarReader() must be implemented.
   * Default: false.
   */
  default boolean supportColumnarReads(InputPartition partition) {
    return false;
  }
}
```

### 2.10 PartitionReader\<T\>

**Package:** `org.apache.spark.sql.connector.read`

The per-partition data reader. `T` is either `InternalRow` (row-based) or `ColumnarBatch` (columnar). Extends `Closeable`.

```java
@Evolving
public interface PartitionReader<T> extends Closeable {

  /** Advance to next record. Returns false when no more records. */
  boolean next() throws IOException;

  /** Return the current record. Same value until next() is called. */
  T get();

  /** Custom task metrics for this reader. Default: empty array. */
  default CustomTaskMetric[] currentMetricsValues() {
    return new CustomTaskMetric[]{};
  }

  /** Initialize metrics from a previous reader (for KeyGroupedPartitioning). */
  default void initMetricsValues(CustomTaskMetric[] metrics) {}
}
```

---

## 3. Pushdown Optimizations

All pushdown interfaces are mix-ins for `ScanBuilder`. The optimizer rule `V2ScanRelationPushDown` applies pushdowns in a defined order before calling `ScanBuilder.build()`.

### 3.1 Pushdown Order

As defined in `V2ScanRelationPushDown.apply()`:

```scala
// sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/V2ScanRelationPushDown.scala
val pushdownRules = Seq[LogicalPlan => LogicalPlan](
  createScanBuilder,
  pushDownSample,
  pushDownFilters,
  pushDownJoin,
  pushDownAggregates,
  pushDownVariants,
  pushDownLimitAndOffset,
  buildScanWithPushedAggregate,
  buildScanWithPushedJoin,
  buildScanWithPushedVariants,
  pruneColumns)
```

### 3.2 SupportsPushDownFilters (V1 Filters)

**Package:** `org.apache.spark.sql.connector.read`

Uses the legacy `org.apache.spark.sql.sources.Filter` API.

```java
@Evolving
public interface SupportsPushDownFilters extends ScanBuilder {

  /**
   * Pushes down filters. Returns filters that still need post-scan evaluation.
   * Filters are ANDed together.
   */
  Filter[] pushFilters(Filter[] filters);

  /**
   * Returns all filters that were successfully pushed to the data source.
   * Includes both fully-pushed and partially-pushed filters (e.g., Parquet row group filters).
   */
  Filter[] pushedFilters();
}
```

### 3.3 SupportsPushDownV2Filters (V2 Predicates)

**Package:** `org.apache.spark.sql.connector.read`

The preferred filter pushdown interface (since 3.3.0). Uses V2 `Predicate` objects, which are more efficient because they avoid internal-to-external data conversion.

```java
@Evolving
public interface SupportsPushDownV2Filters extends ScanBuilder {

  /**
   * Pushes down predicates. Returns predicates that need post-scan evaluation.
   * Predicates are ANDed together.
   */
  Predicate[] pushPredicates(Predicate[] predicates);

  /**
   * Returns all predicates successfully pushed to the data source.
   */
  Predicate[] pushedPredicates();
}
```

### 3.4 SupportsPushDownRequiredColumns

**Package:** `org.apache.spark.sql.connector.read`

Enables column pruning. Spark calls this to tell the data source which columns (and potentially nested fields) are actually needed.

```java
@Evolving
public interface SupportsPushDownRequiredColumns extends ScanBuilder {

  /**
   * Apply column pruning with the given required schema.
   * The Scan.readSchema() must reflect the pruning.
   * Partial pruning is acceptable (e.g., pruning only top-level columns).
   */
  void pruneColumns(StructType requiredSchema);
}
```

### 3.5 SupportsPushDownAggregates

**Package:** `org.apache.spark.sql.connector.read`

Pushes aggregate functions and GROUP BY expressions to the data source.

```java
@Evolving
public interface SupportsPushDownAggregates extends ScanBuilder {

  /**
   * Whether the data source can fully execute this aggregation.
   * If false, Spark will add a re-aggregation on top. Default: false.
   */
  default boolean supportCompletePushDown(Aggregation aggregation) { return false; }

  /**
   * Push the aggregation to the data source.
   * Output column order: grouping columns, then aggregate columns
   * (in the same order as the Aggregation's aggregate functions).
   * Returns true if the aggregation can be pushed.
   */
  boolean pushAggregation(Aggregation aggregation);
}
```

When complete pushdown is not supported, Spark rewrites the logical plan to include a partial aggregation:

```
Aggregate [group_col_0], [min(agg_func_0) AS min(c1), max(agg_func_1) AS max(c1)]
  +- DataSourceV2ScanRelation [group_col_0, agg_func_0, agg_func_1]
```

Partial aggregation pushdown supports only `Min`, `Max`, `Sum`, `Count`, and `CountStar` without DISTINCT.

### 3.6 SupportsPushDownLimit

**Package:** `org.apache.spark.sql.connector.read`

```java
@Evolving
public interface SupportsPushDownLimit extends ScanBuilder {

  /**
   * Push down LIMIT to the data source. Returns true if accepted.
   */
  boolean pushLimit(int limit);

  /**
   * Whether the LIMIT is partially pushed (data source may return more rows than limit).
   * If true, Spark adds a LIMIT on top. Default: true.
   */
  default boolean isPartiallyPushed() { return true; }
}
```

### 3.7 SupportsPushDownTopN

**Package:** `org.apache.spark.sql.connector.read`

Pushes `ORDER BY ... LIMIT n` (Top-N) as a combined operation, which is more efficient than separate sort and limit.

```java
@Evolving
public interface SupportsPushDownTopN extends ScanBuilder {

  /**
   * Push down Top-N (sort orders + limit).
   */
  boolean pushTopN(SortOrder[] orders, int limit);

  /**
   * Whether the Top-N is partially pushed. If true, Spark sorts again. Default: true.
   */
  default boolean isPartiallyPushed() { return true; }
}
```

### 3.8 SupportsPushDownOffset

**Package:** `org.apache.spark.sql.connector.read`

```java
@Evolving
public interface SupportsPushDownOffset extends ScanBuilder {

  /**
   * Push down OFFSET to the data source. Returns true if accepted.
   */
  boolean pushOffset(int offset);
}
```

### 3.9 SupportsPushDownTableSample

**Package:** `org.apache.spark.sql.connector.read`

```java
@Evolving
public interface SupportsPushDownTableSample extends ScanBuilder {

  /**
   * Push down TABLESAMPLE to the data source.
   */
  boolean pushTableSample(
      double lowerBound,
      double upperBound,
      boolean withReplacement,
      long seed);
}
```

### 3.10 SupportsPushDownJoin (Since 4.1.0)

**Package:** `org.apache.spark.sql.connector.read`

Enables the data source to handle join operations natively (e.g., a JDBC connector can push joins to the remote database).

```java
@Evolving
public interface SupportsPushDownJoin extends ScanBuilder {

  /**
   * Whether the other ScanBuilder is compatible for a pushed join
   * (e.g., same JDBC connection parameters).
   */
  boolean isOtherSideCompatibleForJoin(SupportsPushDownJoin other);

  /**
   * Push down the join.
   * Returns true if the join was successfully pushed.
   */
  boolean pushDownJoin(
      SupportsPushDownJoin other,
      JoinType joinType,
      ColumnWithAlias[] leftSideRequiredColumnsWithAliases,
      ColumnWithAlias[] rightSideRequiredColumnsWithAliases,
      Predicate condition
  );

  /** Helper record for column aliasing in joins. */
  record ColumnWithAlias(String colName, String alias) { ... }
}
```

### 3.11 V2ScanRelationPushDown -- The Optimizer Rule

**Source:** `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/V2ScanRelationPushDown.scala`

This is the catalyst optimizer rule that drives all pushdowns. It operates on the logical plan:

1. **`createScanBuilder`** -- Transforms `DataSourceV2Relation` into `ScanBuilderHolder` by calling `table.asReadable.newScanBuilder(options)`.
2. **`pushDownSample`** -- Pushes `Sample` through `SupportsPushDownTableSample`.
3. **`pushDownFilters`** -- Splits `Filter` conditions into conjunctive predicates, translates them, calls `PushDownUtils.pushFilters()`, and wraps remaining filters as post-scan filters.
4. **`pushDownJoin`** -- Detects join patterns between two `ScanBuilderHolder` nodes sharing compatible `SupportsPushDownJoin` builders, and pushes the join.
5. **`pushDownAggregates`** -- Translates `Aggregate` nodes via `DataSourceStrategy.translateAggregation()` and calls `pushAggregation()`.
6. **`pushDownLimitAndOffset`** -- Handles `Limit`, `Offset`, `LimitAndOffset`, and `Sort+Limit` (Top-N) patterns.
7. **`buildScanWithPushedAggregate` / `buildScanWithPushedJoin` / `buildScanWithPushedVariants`** -- Calls `builder.build()` for holders with these pushdowns, converts to `DataSourceV2ScanRelation`.
8. **`pruneColumns`** -- Applies column pruning via `SupportsPushDownRequiredColumns`, calls `builder.build()`, and creates the final `DataSourceV2ScanRelation`.

The intermediate `ScanBuilderHolder` case class tracks all pushed state:

```scala
// sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/V2ScanRelationPushDown.scala
case class ScanBuilderHolder(
    var output: Seq[AttributeReference],
    relation: DataSourceV2Relation,
    builder: ScanBuilder) extends LeafNode {
  var pushedLimit: Option[Int] = None
  var pushedOffset: Option[Int] = None
  var sortOrders: Seq[V2SortOrder] = Seq.empty
  var pushedSample: Option[TableSampleInfo] = None
  var pushedPredicates: Seq[Predicate] = Seq.empty
  var pushedAggregate: Option[Aggregation] = None
  var pushedAggOutputMap: AttributeMap[Expression] = AttributeMap.empty
  var joinedRelations: Seq[DataSourceV2RelationBase] = Seq(relation)
  // ... more fields for join and variant pushdown
}
```

---

## 4. Streaming Reads

### 4.1 SparkDataStream (Base Interface)

**Package:** `org.apache.spark.sql.connector.read.streaming`

The base interface for all streaming data streams. Manages offsets and lifecycle.

```java
@Evolving
public interface SparkDataStream {

  /** Returns the initial offset for a new streaming query. */
  Offset initialOffset();

  /** Deserialize a JSON string into an Offset. */
  Offset deserializeOffset(String json);

  /** Inform the source that data up to `end` has been processed. */
  void commit(Offset end);

  /** Stop and release resources. */
  void stop();
}
```

### 4.2 Offset

**Package:** `org.apache.spark.sql.connector.read.streaming`

Abstract class representing a position in a data stream. Offsets are serialized to JSON for checkpoint storage. Equality is based on JSON string representation.

```java
@Evolving
public abstract class Offset {
  /** JSON-serialized representation used for checkpoint storage. */
  public abstract String json();

  @Override
  public boolean equals(Object obj) { /* based on json() */ }
  @Override
  public int hashCode() { return this.json().hashCode(); }
}
```

### 4.3 MicroBatchStream

**Package:** `org.apache.spark.sql.connector.read.streaming`

For streaming queries with micro-batch mode. The `Scan.toMicroBatchStream(checkpointLocation)` method returns this.

```java
@Evolving
public interface MicroBatchStream extends SparkDataStream {

  /** Returns the most recent offset available. */
  Offset latestOffset();

  /**
   * Returns input partitions for the range [start, end].
   * Called once per micro-batch.
   */
  InputPartition[] planInputPartitions(Offset start, Offset end);

  /** Returns a factory to create PartitionReaders. */
  PartitionReaderFactory createReaderFactory();
}
```

### 4.4 ContinuousStream

**Package:** `org.apache.spark.sql.connector.read.streaming`

For streaming queries with continuous mode (low-latency, epoch-based processing).

```java
@Evolving
public interface ContinuousStream extends SparkDataStream {

  /** Returns input partitions starting from the given offset. */
  InputPartition[] planInputPartitions(Offset start);

  /** Creates a ContinuousPartitionReaderFactory (not a regular PartitionReaderFactory). */
  ContinuousPartitionReaderFactory createContinuousReaderFactory();

  /** Merge per-partition offsets into a single global offset. */
  Offset mergeOffsets(PartitionOffset[] offsets);

  /**
   * Whether the input partitions need to be reconfigured.
   * If true, Spark interrupts and relaunches the job. Default: false.
   */
  default boolean needsReconfiguration() { return false; }
}
```

### 4.5 ContinuousPartitionReaderFactory

A specialization of `PartitionReaderFactory` that returns `ContinuousPartitionReader` instances:

```java
@Evolving
public interface ContinuousPartitionReaderFactory extends PartitionReaderFactory {
  @Override
  ContinuousPartitionReader<InternalRow> createReader(InputPartition partition);

  @Override
  default ContinuousPartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    throw new SparkUnsupportedOperationException(...);
  }
}
```

### 4.6 ContinuousPartitionReader\<T\>

Extends `PartitionReader<T>` with offset tracking for epoch-based checkpointing:

```java
@Evolving
public interface ContinuousPartitionReader<T> extends PartitionReader<T> {

  /**
   * Get the offset of the current record, or the start offset if no records
   * have been read. Used for checkpoint at epoch boundaries.
   */
  PartitionOffset getOffset();
}
```

---

## 5. Catalog Integration

### 5.1 CatalogPlugin

**Package:** `org.apache.spark.sql.connector.catalog`

The marker interface for V2 catalog implementations. Catalogs are registered via Spark configuration:

```
spark.sql.catalog.my_catalog = com.example.MyCatalog
spark.sql.catalog.my_catalog.option1 = value1
```

```java
@Evolving
public interface CatalogPlugin {

  /**
   * Called once after instantiation. Receives the catalog name
   * and any configuration properties with the catalog prefix removed.
   */
  void initialize(String name, CaseInsensitiveStringMap options);

  /** Returns this catalog's name. */
  String name();

  /** Default namespace for this catalog. Default: empty array. */
  default String[] defaultNamespace() { return new String[0]; }
}
```

### 5.2 TableCatalog

**Package:** `org.apache.spark.sql.connector.catalog`

Extends `CatalogPlugin` with table CRUD operations.

```java
@Evolving
public interface TableCatalog extends CatalogPlugin {

  // Reserved property keys
  String PROP_LOCATION = "location";
  String PROP_COMMENT = "comment";
  String PROP_PROVIDER = "provider";
  String PROP_OWNER = "owner";
  String PROP_EXTERNAL = "external";
  String OPTION_PREFIX = "option.";

  /** List table identifiers in a namespace. */
  Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException;

  /** Load table metadata by identifier. */
  Table loadTable(Identifier ident) throws NoSuchTableException;

  /** Load a table with write privileges. */
  default Table loadTable(Identifier ident, Set<TableWritePrivilege> writePrivileges)
      throws NoSuchTableException { return loadTable(ident); }

  /** Load a specific version of a table (time travel). */
  default Table loadTable(Identifier ident, String version) throws NoSuchTableException { ... }

  /** Load a table at a specific timestamp (time travel). */
  default Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException { ... }

  /** Check if a table exists. */
  default boolean tableExists(Identifier ident) { ... }

  /** Create a table. */
  default Table createTable(Identifier ident, TableInfo tableInfo) throws ... { ... }

  /** Alter a table. */
  Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException;

  /** Drop a table. */
  boolean dropTable(Identifier ident);

  /** Rename a table. */
  void renameTable(Identifier oldIdent, Identifier newIdent) throws ...;
}
```

### 5.3 How Catalogs Feed the Read Path

When a SQL query references `my_catalog.db.table`:

1. Spark resolves `my_catalog` to a `CatalogPlugin` instance.
2. Casts it to `TableCatalog` and calls `loadTable(Identifier("db", "table"))`.
3. The returned `Table` (which implements `SupportsRead`) enters the logical plan as a `DataSourceV2Relation`.
4. The optimizer rule `V2ScanRelationPushDown` calls `table.newScanBuilder(options)` and applies pushdowns.

---

## 6. Physical Execution

### 6.1 DataSourceV2ScanRelation (Logical Plan)

**Source:** `sql/catalyst/src/main/scala/org/apache/spark/sql/execution/datasources/v2/DataSourceV2Relation.scala`

A logical plan node created after pushdowns are applied. Contains the finalized `Scan` and output attributes.

```scala
case class DataSourceV2ScanRelation(
    relation: DataSourceV2Relation,
    scan: Scan,
    output: Seq[AttributeReference],
    keyGroupedPartitioning: Option[Seq[Expression]] = None,
    ordering: Option[Seq[SortOrder]] = None) extends LeafNode with NamedRelation
```

Statistics are computed via `SupportsReportStatistics` if the `Scan` implements it:

```scala
override def computeStats(): Statistics = scan match {
  case r: SupportsReportStatistics =>
    val statistics = r.estimateStatistics()
    DataSourceV2Relation.transformV2Stats(statistics, None, conf.defaultSizeInBytes, output)
  case _ =>
    Statistics(sizeInBytes = conf.defaultSizeInBytes)
}
```

### 6.2 BatchScanExec (Physical Plan)

**Source:** `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/BatchScanExec.scala`

The physical plan node for batch scans. Converts a `Scan` to an RDD.

```scala
case class BatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: Scan,
    runtimeFilters: Seq[Expression],
    ordering: Option[Seq[SortOrder]] = None,
    @transient table: Table,
    spjParams: StoragePartitionJoinParams = StoragePartitionJoinParams()
  ) extends DataSourceV2ScanExecBase with KeyGroupedPartitionedScan[InputPartition] {

  @transient lazy val batch: Batch = if (scan == null) null else scan.toBatch

  @transient override lazy val inputPartitions: Seq[InputPartition] =
    batch.planInputPartitions().toImmutableArraySeq

  override lazy val readerFactory: PartitionReaderFactory = batch.createReaderFactory()

  override lazy val inputRDD: RDD[InternalRow] = {
    // ... handles runtime filtering and key-grouped partitioning
    new DataSourceRDD(
      sparkContext, finalPartitions, readerFactory, supportsColumnar, customMetrics)
  }
}
```

Key behaviors:
- **Runtime Filtering:** If `runtimeFilters` are present and the scan implements `SupportsRuntimeV2Filtering`, filters are applied and `toBatch()` is called again to get filtered partitions.
- **Key-Grouped Partitioning:** Supports storage-partition-join optimizations (shuffle-free joins when data is already co-partitioned).

### 6.3 DataSourceRDD

**Source:** `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/DataSourceRDD.scala`

The RDD that reads data from V2 data sources. Each RDD partition may contain multiple `InputPartition` objects (for key-grouped scenarios).

```scala
class DataSourceRDD(
    sc: SparkContext,
    @transient private val inputPartitions: Seq[Seq[InputPartition]],
    partitionReaderFactory: PartitionReaderFactory,
    columnarReads: Boolean,
    customMetrics: Map[String, SQLMetric])
  extends RDD[InternalRow](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    // For each InputPartition in the split:
    //   if columnarReads: factory.createColumnarReader(partition)
    //   else:             factory.createReader(partition)
    // Wraps in MetricsIterator for row/batch counting and bytesRead tracking
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    castPartition(split).inputPartitions.flatMap(_.preferredLocations())
  }
}
```

### 6.4 SupportsReportStatistics and SupportsReportPartitioning

Two additional mix-in interfaces for `Scan`:

```java
// Report statistics to the optimizer (post-pushdown)
public interface SupportsReportStatistics extends Scan {
  Statistics estimateStatistics();
}

// Report partitioning to avoid unnecessary shuffles
public interface SupportsReportPartitioning extends Scan {
  Partitioning outputPartitioning();
}
```

---

## 7. File-Based V2 Sources

Spark provides base classes for file-based V2 data sources that handle common concerns like file listing, partition discovery, split computation, and filter/column pushdown.

### 7.1 FileDataSourceV2

**Source:** `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileDataSourceV2.scala`

A trait extending `TableProvider` and `DataSourceRegister` for built-in file-based sources:

```scala
trait FileDataSourceV2 extends TableProvider with DataSourceRegister {
  /**
   * Returns a V1 FileFormat class for fallback.
   * Users can disable V2 via SQL config and fall back to V1.
   */
  def fallbackFileFormat: Class[_ <: FileFormat]

  override def supportsExternalMetadata(): Boolean = true

  // Template methods for subclasses
  protected def getTable(options: CaseInsensitiveStringMap): Table
  protected def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table
}
```

### 7.2 FileTable

**Source:** `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileTable.scala`

An abstract `Table` implementation for file-based sources. Implements both `SupportsRead` and `SupportsWrite`.

```scala
abstract class FileTable(
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType])
  extends Table with SupportsRead with SupportsWrite {

  lazy val fileIndex: PartitioningAwareFileIndex = { /* file listing and partition discovery */ }
  lazy val dataSchema: StructType = { /* inferred or user-specified */ }
  override lazy val schema: StructType = { /* dataSchema + partitionSchema */ }

  override def capabilities: java.util.Set[TableCapability] =
    java.util.EnumSet.of(BATCH_READ, BATCH_WRITE)

  def inferSchema(files: Seq[FileStatus]): Option[StructType]
  def supportsDataType(dataType: DataType): Boolean = true
  def formatName: String
  def fallbackFileFormat: Class[_ <: FileFormat]
}
```

### 7.3 FileScanBuilder

**Source:** `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileScanBuilder.scala`

The base `ScanBuilder` for file sources. Implements `SupportsPushDownRequiredColumns` and handles partition filter separation.

```scala
abstract class FileScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType)
  extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownCatalystFilters {

  protected var requiredSchema = StructType(dataSchema.fields ++ partitionSchema.fields)
  protected var partitionFilters = Seq.empty[Expression]
  protected var dataFilters = Seq.empty[Expression]
  protected var pushedDataFilters = Array.empty[Filter]

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  protected def readDataSchema(): StructType = {
    // Filters out partition columns from the required schema
  }

  def readPartitionSchema(): StructType = {
    // Only includes required partition columns
  }

  // Subclasses override to push filters to the file format
  protected def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = Array.empty
}
```

### 7.4 FileScan

**Source:** `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileScan.scala`

The base `Scan` + `Batch` implementation for file sources. Handles file splitting and partition computation.

```scala
trait FileScan extends Scan with Batch with SupportsReportStatistics with ... {

  def isSplitable(path: Path): Boolean = false
  def sparkSession: SparkSession
  def fileIndex: PartitioningAwareFileIndex
  def dataSchema: StructType
  def readDataSchema: StructType
  def readPartitionSchema: StructType
  def partitionFilters: Seq[Expression]
  def dataFilters: Seq[Expression]

  protected def partitions: Seq[FilePartition] = {
    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)
    val maxSplitBytes = FilePartition.maxSplitBytes(sparkSession, selectedPartitions)
    // Split files based on maxSplitBytes and create FilePartitions
  }

  override def planInputPartitions(): Array[InputPartition] = partitions.toArray

  override def toBatch: Batch = this

  override def readSchema(): StructType =
    StructType(readDataSchema.fields ++ readPartitionSchema.fields)

  override def estimateStatistics(): Statistics = { /* based on file sizes */ }
}
```

---

## 8. Examples

### 8.1 CSV V2 Source

The CSV V2 source demonstrates the file-based V2 pattern.

**CSVDataSourceV2** (`sql/core/.../v2/csv/CSVDataSourceV2.scala`):

```scala
class CSVDataSourceV2 extends FileDataSourceV2 {
  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[CSVFileFormat]
  override def shortName(): String = "csv"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    CSVTable(tableName, sparkSession, optionsWithoutPaths, paths, None, fallbackFileFormat)
  }
}
```

**CSVTable** (`sql/core/.../v2/csv/CSVTable.scala`):

```scala
case class CSVTable(...) extends FileTable(...) {
  override def newScanBuilder(options: CaseInsensitiveStringMap): CSVScanBuilder =
    CSVScanBuilder(sparkSession, fileIndex, schema, dataSchema, mergedOptions(options))
  override def formatName: String = "CSV"
}
```

**CSVScanBuilder** (`sql/core/.../v2/csv/CSVScanBuilder.scala`):

```scala
case class CSVScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {

  override def build(): CSVScan = CSVScan(
    sparkSession, fileIndex, dataSchema, readDataSchema(), readPartitionSchema(),
    options, pushedDataFilters, partitionFilters, dataFilters)

  override def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = {
    if (sparkSession.sessionState.conf.csvFilterPushDown) {
      StructFilters.pushedFilters(dataFilters, dataSchema)
    } else Array.empty
  }
}
```

**CSVScan** (`sql/core/.../v2/csv/CSVScan.scala`):

```scala
case class CSVScan(...) extends TextBasedFileScan(sparkSession, options) {
  override def isSplitable(path: Path): Boolean = { /* multiLine check */ }

  override def createReaderFactory(): PartitionReaderFactory = {
    CSVPartitionReaderFactory(conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, parsedOptions, actualFilters)
  }
}
```

**CSVPartitionReaderFactory** (`sql/core/.../v2/csv/CSVPartitionReaderFactory.scala`):

```scala
case class CSVPartitionReaderFactory(sqlConf, broadcastedConf, dataSchema,
    readDataSchema, partitionSchema, options, filters) extends FilePartitionReaderFactory {

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val parser = new UnivocityParser(actualDataSchema, actualReadDataSchema, options, filters)
    val iter = CSVDataSource(options).readFile(conf, file, parser, headerChecker, readDataSchema)
    val fileReader = new PartitionReaderFromIterator[InternalRow](iter)
    new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
      partitionSchema, file.partitionValues)
  }
}
```

**Full chain:** `CSVDataSourceV2` -> `CSVTable` -> `CSVScanBuilder` -> `CSVScan` -> `CSVPartitionReaderFactory` -> `PartitionReader`

### 8.2 Kafka Source

The Kafka connector demonstrates a non-file-based V2 source with batch, micro-batch, and continuous support.

**KafkaSourceProvider** (`connector/kafka-0-10-sql/.../KafkaSourceProvider.scala`):

```scala
class KafkaSourceProvider extends ... with SimpleTableProvider {
  override def shortName(): String = "kafka"

  override def getTable(options: CaseInsensitiveStringMap): KafkaTable = {
    val includeHeaders = options.getBoolean(INCLUDE_HEADERS, false)
    new KafkaTable(includeHeaders)
  }
}
```

**KafkaTable** (inner class in KafkaSourceProvider):

```scala
class KafkaTable(includeHeaders: Boolean) extends Table with SupportsRead with SupportsWrite {
  override def name(): String = "KafkaTable"
  override def schema(): StructType = KafkaRecordToRowConverter.kafkaSchema(includeHeaders)

  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(BATCH_READ, BATCH_WRITE, MICRO_BATCH_READ,
      CONTINUOUS_READ, STREAMING_WRITE, ACCEPT_ANY_SCHEMA)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    () => new KafkaScan(options)  // Lambda implementing ScanBuilder
}
```

**KafkaScan** (inner class):

```scala
class KafkaScan(options: CaseInsensitiveStringMap) extends Scan {
  override def readSchema(): StructType =
    KafkaRecordToRowConverter.kafkaSchema(includeHeaders)

  override def toBatch(): Batch = {
    new KafkaBatch(strategy, caseInsensitiveOptions, specifiedKafkaParams,
      failOnDataLoss, startingOffsets, endingOffsets, includeHeaders)
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = { ... }
  override def toContinuousStream(checkpointLocation: String): ContinuousStream = { ... }
}
```

**KafkaBatch** (`connector/kafka-0-10-sql/.../KafkaBatch.scala`):

```scala
class KafkaBatch(...) extends Batch {
  override def planInputPartitions(): Array[InputPartition] = {
    // Uses KafkaOffsetReader to get offset ranges
    val offsetRanges = kafkaOffsetReader.getOffsetRangesFromUnresolvedOffsets(
      startingOffsets, endingOffsets)
    // Each offset range becomes a KafkaBatchInputPartition
    offsetRanges.map { range =>
      new KafkaBatchInputPartition(range, executorKafkaParams,
        pollTimeoutMs, failOnDataLoss, includeHeaders)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = KafkaBatchReaderFactory
}
```

---

## 9. Implementing a New V2 Data Source

Here is a step-by-step guide for implementing a minimal batch-read V2 data source.

### Step 1: Implement TableProvider

```java
public class MyDataSource implements TableProvider {

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    // Return the schema of your data
    return new StructType()
        .add("id", DataTypes.LongType)
        .add("value", DataTypes.StringType);
  }

  @Override
  public Table getTable(StructType schema, Transform[] partitioning,
                        Map<String, String> properties) {
    return new MyTable(schema, properties);
  }
}
```

### Step 2: Implement Table + SupportsRead

```java
public class MyTable implements Table, SupportsRead {

  private final StructType schema;
  private final Map<String, String> properties;

  @Override
  public String name() { return "MyTable"; }

  @Override
  public StructType schema() { return schema; }

  @Override
  public Set<TableCapability> capabilities() {
    return Collections.singleton(TableCapability.BATCH_READ);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new MyScanBuilder(schema, properties);
  }
}
```

### Step 3: Implement ScanBuilder (with optional pushdowns)

```java
public class MyScanBuilder implements ScanBuilder,
    SupportsPushDownRequiredColumns, SupportsPushDownV2Filters {

  private StructType schema;
  private Predicate[] pushedPredicates = new Predicate[0];

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.schema = requiredSchema;
  }

  @Override
  public Predicate[] pushPredicates(Predicate[] predicates) {
    // Accept all predicates; return empty array (none left for post-scan)
    this.pushedPredicates = predicates;
    return new Predicate[0];
  }

  @Override
  public Predicate[] pushedPredicates() {
    return pushedPredicates;
  }

  @Override
  public Scan build() {
    return new MyScan(schema, pushedPredicates);
  }
}
```

### Step 4: Implement Scan + Batch

```java
public class MyScan implements Scan, Batch {

  private final StructType schema;
  private final Predicate[] predicates;

  @Override
  public StructType readSchema() { return schema; }

  @Override
  public Batch toBatch() { return this; }

  @Override
  public InputPartition[] planInputPartitions() {
    // Determine data splits
    return new InputPartition[] {
        new MyInputPartition(0, 1000),
        new MyInputPartition(1000, 2000)
    };
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new MyReaderFactory(schema, predicates);
  }
}
```

### Step 5: Implement InputPartition (serializable!)

```java
public class MyInputPartition implements InputPartition {
  private final long start;
  private final long end;

  public MyInputPartition(long start, long end) {
    this.start = start;
    this.end = end;
  }

  @Override
  public String[] preferredLocations() {
    return new String[0]; // No locality preference
  }
}
```

### Step 6: Implement PartitionReaderFactory (serializable!)

```java
public class MyReaderFactory implements PartitionReaderFactory {
  private final StructType schema;
  private final Predicate[] predicates;

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    MyInputPartition myPartition = (MyInputPartition) partition;
    return new MyPartitionReader(myPartition.start, myPartition.end, schema, predicates);
  }
}
```

### Step 7: Implement PartitionReader

```java
public class MyPartitionReader implements PartitionReader<InternalRow> {
  private long current;
  private final long end;

  @Override
  public boolean next() throws IOException {
    current++;
    return current <= end;
  }

  @Override
  public InternalRow get() {
    // Build and return the current row
    return new GenericInternalRow(new Object[] {
        current, UTF8String.fromString("value_" + current)
    });
  }

  @Override
  public void close() throws IOException {
    // Release any resources
  }
}
```

### Step 8: Register the Data Source

Create the file `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`:

```
com.example.MyDataSource
```

Or reference it directly:

```scala
spark.read.format("com.example.MyDataSource").load()
```

### Summary Checklist

| Concern | Interface | Must Be Serializable? |
|---------|-----------|----------------------|
| Discovery | `TableProvider` | No |
| Metadata | `Table + SupportsRead` | No |
| Pushdown configuration | `ScanBuilder + SupportsPushDown*` | No |
| Logical scan | `Scan` | No |
| Physical planning | `Batch` | No |
| Partition descriptor | `InputPartition` | **Yes** |
| Reader factory | `PartitionReaderFactory` | **Yes** |
| Data reader | `PartitionReader<T>` | No |

---

## Appendix: Key Source File Locations

| Component | Path |
|-----------|------|
| Core read interfaces | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/read/` |
| Catalog interfaces | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/` |
| Streaming interfaces | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/read/streaming/` |
| V2ScanRelationPushDown | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/V2ScanRelationPushDown.scala` |
| BatchScanExec | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/BatchScanExec.scala` |
| DataSourceRDD | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/DataSourceRDD.scala` |
| DataSourceV2Relation | `sql/catalyst/src/main/scala/org/apache/spark/sql/execution/datasources/v2/DataSourceV2Relation.scala` |
| FileDataSourceV2 | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileDataSourceV2.scala` |
| FileTable | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileTable.scala` |
| FileScanBuilder | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileScanBuilder.scala` |
| FileScan | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileScan.scala` |
| CSV V2 source | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/csv/` |
| Kafka connector | `connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/` |
