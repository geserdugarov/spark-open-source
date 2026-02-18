# DataSourceV2 Write Path -- Developer Documentation

**Apache Spark 4.2.0-SNAPSHOT**

This document provides a comprehensive guide for developers implementing the DataSourceV2 write path
in Apache Spark. It covers every interface in the write pipeline, from the table-level entry point
through physical execution on the cluster, including streaming writes, staging catalogs, and
real-world examples from the Kafka and file-based connectors.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Key Interfaces](#2-key-interfaces)
3. [Write Capabilities (Mixin Interfaces)](#3-write-capabilities-mixin-interfaces)
4. [Two-Phase Commit Protocol](#4-two-phase-commit-protocol)
5. [Streaming Writes](#5-streaming-writes)
6. [Distribution and Ordering Requirements](#6-distribution-and-ordering-requirements)
7. [Logical Plans](#7-logical-plans)
8. [Physical Execution](#8-physical-execution)
9. [Staging Catalog (Atomic CTAS/RTAS)](#9-staging-catalog-atomic-ctasrtas)
10. [DataFrameWriterV2 API](#10-dataframewriterv2-api)
11. [Delta Writes (Row-Level Operations)](#11-delta-writes-row-level-operations)
12. [Real-World Examples](#12-real-world-examples)
13. [Implementing a New V2 Write Data Source](#13-implementing-a-new-v2-write-data-source)

---

## 1. Overview

The DataSourceV2 write architecture follows a layered pipeline that separates logical planning,
physical planning, and distributed execution:

```
Table (SupportsWrite)
  |
  v
WriteBuilder  (configured via capability mixins)
  |
  v
Write  (logical representation, shared between batch and streaming)
  |
  +---> toBatch() ---> BatchWrite (driver-side coordination)
  |                       |
  |                       +---> createBatchWriterFactory() ---> DataWriterFactory (serialized to executors)
  |                       |                                        |
  |                       |                                        +---> createWriter() ---> DataWriter (per-partition)
  |                       |                                                                    |
  |                       |                                                                    +---> write(record)
  |                       |                                                                    +---> commit() -> WriterCommitMessage
  |                       |                                                                    +---> abort()
  |                       |
  |                       +---> commit(WriterCommitMessage[])   (driver-side final commit)
  |                       +---> abort(WriterCommitMessage[])    (driver-side abort/cleanup)
  |
  +---> toStreaming() ---> StreamingWrite (epoch-based variant)
```

**Key design principles:**

- **Separation of concerns**: Logical write configuration (WriteBuilder) is separated from physical
  execution (BatchWrite/DataWriter).
- **Serialization boundary**: The `DataWriterFactory` is serialized and shipped to executors; the
  `DataWriter` itself runs on executors and does not need to be serializable.
- **Two-phase commit**: Individual `DataWriter` instances on executors produce `WriterCommitMessage`
  objects, which are collected on the driver and passed to `BatchWrite.commit()` for the final
  atomic commit.
- **Capability-based**: Tables declare their capabilities (`BATCH_WRITE`, `TRUNCATE`,
  `OVERWRITE_BY_FILTER`, etc.) and mix in the appropriate builder interfaces.

---

## 2. Key Interfaces

All interfaces reside in the package `org.apache.spark.sql.connector.write` (and sub-packages)
under `sql/catalyst/src/main/java/`.

### 2.1 SupportsWrite

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/SupportsWrite.java`

The entry point for all V2 writes. A `Table` that supports writing must implement this mix-in.

```java
@Evolving
public interface SupportsWrite extends Table {
    /**
     * Returns a WriteBuilder which can be used to create BatchWrite.
     * Spark will call this method to configure each data source write.
     */
    WriteBuilder newWriteBuilder(LogicalWriteInfo info);
}
```

### 2.2 LogicalWriteInfo

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/LogicalWriteInfo.java`

Provides logical metadata about the write operation to the `WriteBuilder`.

```java
@Evolving
public interface LogicalWriteInfo {
    /** The options that the user specified when writing the dataset. */
    CaseInsensitiveStringMap options();

    /** A unique string of the query, usable to identify the query across restarts. */
    String queryId();

    /** The schema of the input data from Spark to the data source. */
    StructType schema();

    /** The schema of the ID columns (for row-level operations). */
    default Optional<StructType> rowIdSchema();

    /** The schema of the input metadata (for row-level operations). */
    default Optional<StructType> metadataSchema();
}
```

### 2.3 WriteBuilder

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/WriteBuilder.java`

Builds a `Write` instance. Implementations can mix in capability interfaces (`SupportsTruncate`,
`SupportsOverwrite`, etc.) to support different write modes. By default the write configured by
this builder appends data without affecting existing data.

```java
@Evolving
public interface WriteBuilder {
    /**
     * Returns a logical Write shared between batch and streaming.
     * @since 3.2.0
     */
    default Write build() {
        return new Write() {
            @Override public BatchWrite toBatch() { return buildForBatch(); }
            @Override public StreamingWrite toStreaming() { return buildForStreaming(); }
        };
    }

    /** @deprecated use build() instead. */
    @Deprecated(since = "3.2.0")
    default BatchWrite buildForBatch() { /* throws */ }

    /** @deprecated use build() instead. */
    @Deprecated(since = "3.2.0")
    default StreamingWrite buildForStreaming() { /* throws */ }
}
```

### 2.4 Write

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/Write.java`

A logical representation of a data source write, shared between batch and streaming. Data sources
must implement the method matching the capability declared by their `Table`:

- `toBatch()` if the table returns `TableCapability.BATCH_WRITE`
- `toStreaming()` if the table returns `TableCapability.STREAMING_WRITE`

```java
@Evolving
public interface Write {
    /** Returns the description associated with this write. */
    default String description() { return this.getClass().toString(); }

    /** Returns a BatchWrite to write data to batch source. */
    default BatchWrite toBatch() { /* throws UnsupportedOperationException */ }

    /** Returns a StreamingWrite to write data to streaming source. */
    default StreamingWrite toStreaming() { /* throws UnsupportedOperationException */ }

    /** Returns an array of supported custom metrics with name and description. */
    default CustomMetric[] supportedCustomMetrics() { return new CustomMetric[]{}; }

    /** Returns custom metrics collected at the driver side only. */
    default CustomTaskMetric[] reportDriverMetrics() { return new CustomTaskMetric[]{}; }
}
```

### 2.5 BatchWrite

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/BatchWrite.java`

Defines how to write data for batch processing. Runs on the driver and coordinates the distributed
write. The contract is:

1. Create a writer factory via `createBatchWriterFactory(PhysicalWriteInfo)`.
2. Serialize and send the factory to all partitions.
3. Each partition creates a `DataWriter`, writes data, and calls `commit()` or `abort()`.
4. On success of all writers, the driver calls `commit(WriterCommitMessage[])`.
5. On failure, the driver calls `abort(WriterCommitMessage[])`.

```java
@Evolving
public interface BatchWrite {
    /**
     * Creates a writer factory which will be serialized and sent to executors.
     * @param info Physical information about the input data.
     */
    DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info);

    /**
     * Returns whether Spark should use the commit coordinator to ensure
     * that at most one task for each partition commits.
     * @return true if commit coordinator should be used, false otherwise.
     */
    default boolean useCommitCoordinator() { return true; }

    /**
     * Handles a commit message on receiving from a successful data writer.
     * Called per-writer as messages arrive, before the final commit.
     */
    default void onDataWriterCommit(WriterCommitMessage message) {}

    /**
     * Commits this writing job with a list of commit messages from successful data writers.
     */
    void commit(WriterCommitMessage[] messages);

    /**
     * Commits with a list of commit messages and a WriteSummary.
     * Default implementation delegates to commit(messages).
     * @since 4.1.0
     */
    default void commit(WriterCommitMessage[] messages, WriteSummary summary) {
        commit(messages);
    }

    /**
     * Aborts this writing job. The given messages may have null slots
     * (best-effort cleanup).
     */
    void abort(WriterCommitMessage[] messages);
}
```

### 2.6 PhysicalWriteInfo

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/PhysicalWriteInfo.java`

Physical information about the input data that will be written.

```java
@Evolving
public interface PhysicalWriteInfo {
    /** The number of partitions of the input data that is going to be written. */
    int numPartitions();
}
```

### 2.7 DataWriterFactory

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/DataWriterFactory.java`

Created by `BatchWrite.createBatchWriterFactory()`, serialized and sent to executors.
**Must be `Serializable`.**

```java
@Evolving
public interface DataWriterFactory extends Serializable {
    /**
     * Returns a data writer to do the actual writing work.
     *
     * Note: Spark will reuse the same data object instance when sending data
     * to the data writer, for better performance. Data writers are responsible
     * for defensive copies if necessary.
     *
     * @param partitionId A unique id of the RDD partition.
     * @param taskId The task id returned by TaskContext.taskAttemptId().
     *               Different for retries and speculative tasks.
     */
    DataWriter<InternalRow> createWriter(int partitionId, long taskId);
}
```

### 2.8 DataWriter\<T\>

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/DataWriter.java`

The per-partition writer running on executors. One Spark task has one exclusive `DataWriter`, so
there is no thread-safety concern. Currently `T` can only be `InternalRow`.

```java
@Evolving
public interface DataWriter<T> extends Closeable {
    /**
     * Writes one record.
     * If this method fails, abort() will be called.
     */
    void write(T record) throws IOException;

    /**
     * Writes one record with metadata (for row-level operations).
     * @since 4.0.0
     */
    default void write(T metadata, T record) throws IOException { write(record); }

    /**
     * Writes all records provided by the given iterator.
     * @since 4.0.0
     */
    default void writeAll(Iterator<T> records) throws IOException {
        while (records.hasNext()) { write(records.next()); }
    }

    /**
     * Commits this writer after all records are written successfully.
     * Returns a WriterCommitMessage which will be sent back to the driver
     * and passed to BatchWrite.commit(WriterCommitMessage[]).
     *
     * Written data should only be visible to readers after
     * BatchWrite.commit(WriterCommitMessage[]) succeeds.
     */
    WriterCommitMessage commit() throws IOException;

    /**
     * Aborts this writer if it has failed.
     * Implementations should clean up data for already written records.
     */
    void abort() throws IOException;

    /** Returns an array of custom task metrics. */
    default CustomTaskMetric[] currentMetricsValues() { return new CustomTaskMetric[]{}; }
}
```

### 2.9 WriterCommitMessage

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/WriterCommitMessage.java`

A marker interface. Data sources define their own message class. Messages are produced on executors
by `DataWriter.commit()` and consumed on the driver by `BatchWrite.commit()`.

```java
@Evolving
public interface WriterCommitMessage extends Serializable {}
```

---

## 3. Write Capabilities (Mixin Interfaces)

These interfaces are mixed into a `WriteBuilder` implementation to indicate which write modes the
data source supports. The `Table.capabilities()` method must return the corresponding
`TableCapability` enum values.

### 3.1 SupportsTruncate

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/SupportsTruncate.java`

Indicates the table supports truncation (removing all existing data and replacing it with the
new write's data). Corresponds to `TableCapability.TRUNCATE`.

```java
@Evolving
public interface SupportsTruncate extends WriteBuilder {
    /**
     * Configures a write to replace all existing data
     * with data committed in the write.
     * @return this write builder for method chaining
     */
    WriteBuilder truncate();
}
```

### 3.2 SupportsOverwriteV2

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/SupportsOverwriteV2.java`

The newer (since 3.4.0) predicate-based overwrite interface. Extends both `WriteBuilder` and
`SupportsTruncate`. Corresponds to `TableCapability.OVERWRITE_BY_FILTER`.

```java
@Evolving
public interface SupportsOverwriteV2 extends WriteBuilder, SupportsTruncate {
    /**
     * Checks whether it is possible to overwrite data matching the predicates.
     * Predicates are ANDed together.
     */
    default boolean canOverwrite(Predicate[] predicates) { return true; }

    /**
     * Configures a write to replace data matching the filters.
     * Filters are ANDed together.
     * @return this write builder for method chaining
     */
    WriteBuilder overwrite(Predicate[] predicates);

    /** Default truncate() implementation: overwrite with AlwaysTrue predicate. */
    @Override
    default WriteBuilder truncate() {
        return overwrite(new Predicate[] { new AlwaysTrue() });
    }
}
```

### 3.3 SupportsOverwrite (Legacy V1 Filter-Based)

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/SupportsOverwrite.java`

The older filter-based overwrite interface. Extends `SupportsOverwriteV2` and adds V1 `Filter`
methods. New implementations should prefer `SupportsOverwriteV2`.

```java
@Evolving
public interface SupportsOverwrite extends SupportsOverwriteV2 {
    /** Checks if overwrite is possible using V1 filters. */
    default boolean canOverwrite(Filter[] filters) { return true; }

    /** Configures a write to replace data matching V1 filters (ANDed). */
    WriteBuilder overwrite(Filter[] filters);

    /** Bridge: converts V2 Predicates to V1 Filters and delegates. */
    default boolean canOverwrite(Predicate[] predicates) { /* converts and delegates */ }
    default WriteBuilder overwrite(Predicate[] predicates) { /* converts and delegates */ }

    /** Truncate implemented as overwrite(AlwaysTrue). */
    @Override
    default WriteBuilder truncate() {
        return overwrite(new Filter[] { AlwaysTrue$.MODULE$ });
    }
}
```

### 3.4 SupportsDynamicOverwrite

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/SupportsDynamicOverwrite.java`

Indicates support for dynamic partition overwrite -- the Hive `INSERT OVERWRITE ... PARTITION`
behavior. Only partitions that appear in the write data are replaced; partitions without new data
remain unchanged. Corresponds to `TableCapability.OVERWRITE_DYNAMIC`.

```java
@Evolving
public interface SupportsDynamicOverwrite extends WriteBuilder {
    /**
     * Configures a write to dynamically replace partitions
     * with data committed in the write.
     * @return this write builder for method chaining
     */
    WriteBuilder overwriteDynamicPartitions();
}
```

### 3.5 TableCapability Enum (Write-Related Values)

The `Table.capabilities()` method must return a `Set<TableCapability>` that includes the relevant
values:

| Capability | Description |
|---|---|
| `BATCH_WRITE` | Supports appending data in batch mode. May also support TRUNCATE, OVERWRITE_BY_FILTER, OVERWRITE_DYNAMIC. |
| `STREAMING_WRITE` | Supports appending data in streaming execution mode. |
| `TRUNCATE` | Table can be truncated in a write operation. |
| `OVERWRITE_BY_FILTER` | Table can replace existing data matching a filter. |
| `OVERWRITE_DYNAMIC` | Table can dynamically replace existing data partitions. |
| `ACCEPT_ANY_SCHEMA` | Table accepts input of any schema (validation is done by the data source). |
| `V1_BATCH_WRITE` | Falls back to V1 write path. |

---

## 4. Two-Phase Commit Protocol

The V2 write path uses a two-phase commit protocol to ensure data consistency across distributed
writes.

### Phase 1: Executor-Side Writes

Each Spark task on an executor:

1. Creates a `DataWriter` via `DataWriterFactory.createWriter(partitionId, taskId)`.
2. Calls `writer.write(record)` for each record in the partition.
3. On success: calls `writer.commit()` which returns a `WriterCommitMessage`.
4. On failure: calls `writer.abort()` to clean up partial writes.
5. Always calls `writer.close()` for resource cleanup.

### Phase 2: Driver-Side Coordination

On the driver:

1. As each task completes, `BatchWrite.onDataWriterCommit(message)` is called.
2. After all tasks succeed, `BatchWrite.commit(WriterCommitMessage[])` is called with all messages.
3. If any task fails permanently (after retries), `BatchWrite.abort(WriterCommitMessage[])` is
   called. The messages array may contain null slots for tasks that never committed.

### Commit Coordinator

By default, `BatchWrite.useCommitCoordinator()` returns `true`. When enabled, Spark uses the
`OutputCommitCoordinator` to ensure that at most one task attempt per partition commits. This is
essential for correctness when speculative execution is enabled.

```
Executor (Partition 0, Task Attempt A):  write -> commit() -> WriterCommitMessage
Executor (Partition 0, Task Attempt B):  write -> [coordinator denies] -> abort()
Executor (Partition 1, Task Attempt A):  write -> commit() -> WriterCommitMessage
                    |
                    v
Driver:  BatchWrite.commit([msg_partition_0, msg_partition_1])
```

When `useCommitCoordinator()` returns `false` (e.g., for data sources like Kafka or file-based
sources that handle idempotency differently), multiple task attempts for the same partition may
commit, and only one message per partition is passed to `commit()`. This mode is useful when the
underlying system is inherently idempotent or uses its own commit protocol (e.g., Hadoop
`FileCommitProtocol`).

### Retry Behavior

- Spark **will** retry failed writing tasks (up to the configured maximum retries).
- Spark **will not** retry failed writing jobs. Application-level retry is the user's responsibility.
- Different retries receive different `taskId` values from `TaskContext.taskAttemptId()`.

---

## 5. Streaming Writes

### 5.1 StreamingWrite

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/streaming/StreamingWrite.java`

The streaming equivalent of `BatchWrite`. Commits are organized by epoch (micro-batch ID).

```java
@Evolving
public interface StreamingWrite {
    /**
     * Creates a writer factory which will be serialized and sent to executors.
     */
    StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info);

    /**
     * Returns whether Spark should use the commit coordinator.
     */
    default boolean useCommitCoordinator() { return true; }

    /**
     * Commits this writing job for the specified epoch.
     * Implementations must ensure that multiple commits for the same epoch
     * are idempotent (for exactly-once semantics).
     */
    void commit(long epochId, WriterCommitMessage[] messages);

    /**
     * Aborts this writing job for the specified epoch.
     */
    void abort(long epochId, WriterCommitMessage[] messages);
}
```

### 5.2 StreamingDataWriterFactory

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/streaming/StreamingDataWriterFactory.java`

Like `DataWriterFactory` but includes an `epochId` parameter for identifying the streaming
micro-batch.

```java
@Evolving
public interface StreamingDataWriterFactory extends Serializable {
    /**
     * @param partitionId A unique id of the RDD partition.
     * @param taskId      The task attempt id.
     * @param epochId     A monotonically increasing id for streaming micro-batches.
     */
    DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId);
}
```

### 5.3 Streaming Write Protocol

The streaming write follows the same two-phase commit pattern, but repeats for each epoch:

```
For each epoch (micro-batch):
  1. createStreamingWriterFactory(info) -> factory
  2. For each partition:
       factory.createWriter(partitionId, taskId, epochId) -> writer
       writer.write(record) ...
       writer.commit() -> WriterCommitMessage
  3. streamingWrite.commit(epochId, messages[])
     (or streamingWrite.abort(epochId, messages[]) on failure)
```

---

## 6. Distribution and Ordering Requirements

### RequiresDistributionAndOrdering

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/RequiresDistributionAndOrdering.java`

A `Write` implementation can implement this interface to request that Spark distribute and/or
sort incoming records before passing them to data writers.

```java
@Evolving
public interface RequiresDistributionAndOrdering extends Write {
    /** Returns the distribution required by this write. */
    Distribution requiredDistribution();

    /** Whether the distribution is strictly required or best effort. Default: true. */
    default boolean distributionStrictlyRequired() { return true; }

    /**
     * Required number of partitions. Any value less than 1 means no requirement.
     * Cannot be combined with advisoryPartitionSizeInBytes.
     */
    default int requiredNumPartitions() { return 0; }

    /**
     * Advisory shuffle partition size in bytes (for AQE).
     * Cannot be combined with requiredNumPartitions.
     */
    default long advisoryPartitionSizeInBytes() { return 0; }

    /** Returns the ordering required within each partition. */
    SortOrder[] requiredOrdering();
}
```

When a `Write` implements this interface, Spark will insert shuffle and sort operations into the
physical plan before the write to satisfy the requested distribution and ordering.

---

## 7. Logical Plans

**File**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/v2Commands.scala`

All V2 write logical plans extend the `V2WriteCommand` trait:

```scala
trait V2WriteCommand extends UnaryCommand with KeepAnalyzedQuery with CTEInChildren {
  def table: NamedRelation
  def query: LogicalPlan
  def isByName: Boolean
  // ...
}
```

### 7.1 AppendData

Appends rows to an existing table.

```scala
case class AppendData(
    table: NamedRelation,
    query: LogicalPlan,
    writeOptions: Map[String, String],
    isByName: Boolean,
    write: Option[Write] = None,
    analyzedQuery: Option[LogicalPlan] = None) extends V2WriteCommand
```

Factory methods:
- `AppendData.byName(table, df, writeOptions)` -- resolves columns by name
- `AppendData.byPosition(table, df, writeOptions)` -- resolves columns by ordinal position

### 7.2 OverwriteByExpression

Overwrites rows matching a delete expression, then appends new data.

```scala
case class OverwriteByExpression(
    table: NamedRelation,
    deleteExpr: Expression,    // filter for rows to delete (e.g., Literal.TrueLiteral for truncate)
    query: LogicalPlan,
    writeOptions: Map[String, String],
    isByName: Boolean,
    write: Option[Write] = None,
    analyzedQuery: Option[LogicalPlan] = None) extends V2WriteCommand
```

When `deleteExpr` is `Literal.TrueLiteral`, this is equivalent to a truncate-and-append (i.e.,
`SaveMode.Overwrite`).

### 7.3 OverwritePartitionsDynamic

Dynamically overwrites partitions -- only partitions with new data are replaced.

```scala
case class OverwritePartitionsDynamic(
    table: NamedRelation,
    query: LogicalPlan,
    writeOptions: Map[String, String],
    isByName: Boolean,
    write: Option[Write] = None) extends V2WriteCommand
```

### 7.4 CreateTableAsSelect (CTAS)

Creates a new table with the schema of the query and populates it with the query result.

```scala
case class CreateTableAsSelect(
    name: LogicalPlan,
    partitioning: Seq[Transform],
    query: LogicalPlan,
    tableSpec: TableSpecBase,
    writeOptions: Map[String, String],
    ignoreIfExists: Boolean,
    isAnalyzed: Boolean = false) extends V2CreateTableAsSelectPlan
```

### 7.5 ReplaceTableAsSelect (RTAS)

Replaces an existing table's schema and data with the query result.

```scala
case class ReplaceTableAsSelect(
    name: LogicalPlan,
    partitioning: Seq[Transform],
    query: LogicalPlan,
    tableSpec: TableSpecBase,
    writeOptions: Map[String, String],
    orCreate: Boolean,      // if true, acts as CREATE OR REPLACE
    isAnalyzed: Boolean = false) extends V2CreateTableAsSelectPlan
```

---

## 8. Physical Execution

**File**: `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/WriteToDataSourceV2Exec.scala`

### 8.1 Physical Plan Nodes

The logical plans are converted to physical plan nodes by `DataSourceV2Strategy`:

| Logical Plan | Physical Plan (non-atomic) | Physical Plan (atomic/staging) |
|---|---|---|
| `AppendData` | `AppendDataExec` | `AppendDataExec` |
| `OverwriteByExpression` | `OverwriteByExpressionExec` | `OverwriteByExpressionExec` |
| `OverwritePartitionsDynamic` | `OverwritePartitionsDynamicExec` | `OverwritePartitionsDynamicExec` |
| `CreateTableAsSelect` | `CreateTableAsSelectExec` | `AtomicCreateTableAsSelectExec` |
| `ReplaceTableAsSelect` | `ReplaceTableAsSelectExec` | `AtomicReplaceTableAsSelectExec` |

The core write execution nodes for existing tables share the `V2ExistingTableWriteExec` trait:

```scala
case class AppendDataExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write) extends V2ExistingTableWriteExec

case class OverwriteByExpressionExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write) extends V2ExistingTableWriteExec

case class OverwritePartitionsDynamicExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write) extends V2ExistingTableWriteExec
```

### 8.2 V2ExistingTableWriteExec

The shared trait that handles writing to an existing V2 table:

```scala
trait V2ExistingTableWriteExec extends V2TableWriteExec {
  def refreshCache: () => Unit
  def write: Write

  override protected def run(): Seq[InternalRow] = {
    val writtenRows = try {
      writeWithV2(write.toBatch)     // Calls toBatch() to get the BatchWrite
    } finally {
      postDriverMetrics()            // Reports driver-side custom metrics
    }
    refreshCache()
    writtenRows
  }
}
```

### 8.3 V2TableWriteExec and writeWithV2

The `V2TableWriteExec` trait contains the core distributed write logic in `writeWithV2`:

```scala
trait V2TableWriteExec extends V2CommandExec with UnaryExecNode {
  def query: SparkPlan
  def writingTask: WritingSparkTask[_] = DataWritingSparkTask

  protected def writeWithV2(batchWrite: BatchWrite): Seq[InternalRow] = {
    val rdd = query.execute()  // or a dummy single-partition RDD if 0 partitions

    val writerFactory = batchWrite.createBatchWriterFactory(
      PhysicalWriteInfoImpl(rdd.getNumPartitions))
    val useCommitCoordinator = batchWrite.useCommitCoordinator
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)

    try {
      sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[InternalRow]) =>
          writingTask.run(writerFactory, context, iter, useCommitCoordinator, writeMetrics),
        rdd.partitions.indices,
        (index, result: DataWritingSparkTaskResult) => {
          messages(index) = result.writerCommitMessage
          batchWrite.onDataWriterCommit(result.writerCommitMessage)
        }
      )

      batchWrite.commit(messages)   // Final driver-side commit
    } catch {
      case cause: Throwable =>
        batchWrite.abort(messages)  // Driver-side abort on failure
        throw cause
    }
    Nil
  }
}
```

**Important detail**: If the input RDD has zero partitions, Spark creates a dummy single-partition
RDD to ensure at least one write task executes. This allows data sources to write metadata even
when there is no data (SPARK-23271).

### 8.4 WritingSparkTask

The `WritingSparkTask` runs on each executor to process one partition:

```scala
trait WritingSparkTask[W <: DataWriter[InternalRow]] extends Logging with Serializable {
  protected def write(writer: W, iter: java.util.Iterator[InternalRow]): Unit

  def run(
      writerFactory: DataWriterFactory,
      context: TaskContext,
      iter: Iterator[InternalRow],
      useCommitCoordinator: Boolean,
      customMetrics: Map[String, SQLMetric]): DataWritingSparkTaskResult = {

    val dataWriter = writerFactory.createWriter(
      context.partitionId(), context.taskAttemptId())

    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      write(dataWriter, iter)   // Write all records

      val msg = if (useCommitCoordinator) {
        // Ask the commit coordinator for permission to commit
        val coordinator = SparkEnv.get.outputCommitCoordinator
        val commitAuthorized = coordinator.canCommit(
          stageId, stageAttempt, partId, attemptId)
        if (commitAuthorized) {
          dataWriter.commit()
        } else {
          throw commitDeniedException  // Triggers abort
        }
      } else {
        dataWriter.commit()
      }

      DataWritingSparkTaskResult(count, msg)
    })(
      catchBlock = { dataWriter.abort() },
      finallyBlock = { dataWriter.close() }
    )
  }
}
```

The default implementation, `DataWritingSparkTask`, delegates to `writer.writeAll(iter)`:

```scala
object DataWritingSparkTask extends WritingSparkTask[DataWriter[InternalRow]] {
  override protected def write(
      writer: DataWriter[InternalRow],
      iter: java.util.Iterator[InternalRow]): Unit = {
    writer.writeAll(iter)
  }
}
```

---

## 9. Staging Catalog (Atomic CTAS/RTAS)

### 9.1 StagingTableCatalog

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/StagingTableCatalog.java`

An optional mix-in for `TableCatalog` implementations that enables atomic CREATE TABLE AS SELECT
and REPLACE TABLE AS SELECT operations by staging the table creation before committing it along
with the data.

Without staging, RTAS would:
1. Drop the existing table
2. Create a new table
3. Write data

If step 3 fails, the original table is lost. With staging, the table metadata is only committed
after the write succeeds.

```java
@Evolving
public interface StagingTableCatalog extends TableCatalog {
    /**
     * Stage the creation of a table (for CTAS).
     * @throws TableAlreadyExistsException If the table already exists
     */
    StagedTable stageCreate(Identifier ident, TableInfo tableInfo)
        throws TableAlreadyExistsException, NoSuchNamespaceException;

    /**
     * Stage the replacement of a table (for RTAS).
     * Fails if the table does not exist at commit time.
     */
    StagedTable stageReplace(Identifier ident, TableInfo tableInfo)
        throws NoSuchNamespaceException, NoSuchTableException;

    /**
     * Stage the creation or replacement of a table (for CREATE OR REPLACE ... AS SELECT).
     * Creates the table if it does not exist.
     */
    StagedTable stageCreateOrReplace(Identifier ident, TableInfo tableInfo)
        throws NoSuchNamespaceException;

    /** Custom metrics supported by the catalog for staged operations. */
    default CustomMetric[] supportedCustomMetrics() { return new CustomMetric[0]; }
}
```

### 9.2 StagedTable

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/StagedTable.java`

A table that has been staged but not yet committed. Usually implements `SupportsWrite` so Spark
can write data to it. After `BatchWrite.commit()` succeeds, Spark calls
`StagedTable.commitStagedChanges()` to atomically publish both the data and the table metadata.

```java
@Evolving
public interface StagedTable extends Table {
    /** Finalize the creation or replacement of this table. */
    void commitStagedChanges();

    /** Abort the changes that were staged. */
    void abortStagedChanges();

    /** Retrieve driver metrics after a commit. */
    default CustomTaskMetric[] reportDriverMetrics() throws RuntimeException {
        return new CustomTaskMetric[0];
    }
}
```

### 9.3 Atomic vs. Non-Atomic Execution

**AtomicCreateTableAsSelectExec** (uses `StagingTableCatalog`):

```scala
case class AtomicCreateTableAsSelectExec(
    catalog: StagingTableCatalog, ...) extends V2CreateTableAsSelectBaseExec {
  override protected def run(): Seq[InternalRow] = {
    val stagedTable = catalog.stageCreate(ident, tableInfo)
    writeToTable(catalog, stagedTable, ...)
    // writeToTable calls: write data -> BatchWrite.commit() -> StagedTable.commitStagedChanges()
    // On failure: StagedTable.abortStagedChanges()
  }
}
```

**CreateTableAsSelectExec** (non-atomic, standard `TableCatalog`):

```scala
case class CreateTableAsSelectExec(
    catalog: TableCatalog, ...) extends V2CreateTableAsSelectBaseExec {
  override protected def run(): Seq[InternalRow] = {
    val table = catalog.createTable(ident, tableInfo)  // Table created immediately
    writeToTable(catalog, table, ...)
    // On failure: catalog.dropTable(ident)  -- table is lost!
  }
}
```

**ReplaceTableAsSelectExec** (non-atomic, potentially unsafe):

```scala
case class ReplaceTableAsSelectExec(
    catalog: TableCatalog, ...) extends V2CreateTableAsSelectBaseExec {
  override protected def run(): Seq[InternalRow] = {
    // WARNING: Drops existing table first -- irrecoverable if write fails
    catalog.dropTable(ident)
    val table = catalog.createTable(ident, tableInfo)
    writeToTable(catalog, table, ...)
  }
}
```

---

## 10. DataFrameWriterV2 API

**API File**: `sql/api/src/main/scala/org/apache/spark/sql/DataFrameWriterV2.scala`
**Implementation**: `sql/core/src/main/scala/org/apache/spark/sql/classic/DataFrameWriterV2.scala`

The user-facing Scala/Java API for V2 writes. Accessed via `df.writeTo("tableName")`.

### Core Operations

```scala
// Append data to an existing table
df.writeTo("catalog.db.table")
  .option("key", "value")
  .append()

// Overwrite rows matching a condition
df.writeTo("catalog.db.table")
  .overwrite(col("date") === "2024-01-01")

// Dynamic partition overwrite (Hive-style)
df.writeTo("catalog.db.table")
  .overwritePartitions()

// Create a new table with data
df.writeTo("catalog.db.table")
  .using("parquet")
  .partitionedBy(col("date"))
  .tableProperty("key", "value")
  .create()

// Replace an existing table
df.writeTo("catalog.db.table")
  .using("parquet")
  .replace()

// Create or replace
df.writeTo("catalog.db.table")
  .using("parquet")
  .createOrReplace()

// Cluster by columns (since 4.0.0)
df.writeTo("catalog.db.table")
  .using("parquet")
  .clusterBy("col1", "col2")
  .create()
```

### Mapping to Logical Plans

| API Method | Logical Plan |
|---|---|
| `append()` | `AppendData.byName(...)` |
| `overwrite(condition)` | `OverwriteByExpression.byName(...)` |
| `overwritePartitions()` | `OverwritePartitionsDynamic.byName(...)` |
| `create()` | `CreateTableAsSelect(...)` |
| `replace()` | `ReplaceTableAsSelect(..., orCreate=false)` |
| `createOrReplace()` | `ReplaceTableAsSelect(..., orCreate=true)` |

---

## 11. Delta Writes (Row-Level Operations)

For advanced row-level operations (MERGE INTO, UPDATE, DELETE), Spark 3.4+ introduced delta write
interfaces.

### DeltaWrite

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/DeltaWrite.java`

```java
@Experimental
public interface DeltaWrite extends Write {
    @Override
    default DeltaBatchWrite toBatch() { /* throws */ }
}
```

### DeltaWriter\<T\>

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/DeltaWriter.java`

Extends `DataWriter<T>` with row-level mutation operations:

```java
@Experimental
public interface DeltaWriter<T> extends DataWriter<T> {
    /** Deletes a row identified by the given row ID. */
    void delete(T metadata, T id) throws IOException;

    /** Updates a row identified by the given row ID with new values. */
    void update(T metadata, T id, T row) throws IOException;

    /** Reinserts a row (the insert portion of a split update). @since 4.0.0 */
    default void reinsert(T metadata, T row) throws IOException { insert(row); }

    /** Inserts a new row. */
    void insert(T row) throws IOException;

    /** Default write() delegates to insert(). */
    @Override
    default void write(T row) throws IOException { insert(row); }
}
```

### MergeSummary / WriteSummary

**File**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/MergeSummary.java`

Provides informational statistics about a MERGE operation to `BatchWrite.commit()` (since 4.1.0):

```java
@Evolving
public interface MergeSummary extends WriteSummary {
    long numTargetRowsCopied();
    long numTargetRowsDeleted();
    long numTargetRowsUpdated();
    long numTargetRowsInserted();
    long numTargetRowsMatchedUpdated();
    long numTargetRowsMatchedDeleted();
    long numTargetRowsNotMatchedBySourceUpdated();
    long numTargetRowsNotMatchedBySourceDeleted();
}
```

---

## 12. Real-World Examples

### 12.1 Kafka Connector

The Kafka connector is a clean, minimal example of the V2 write path.

**Full chain**: `KafkaTable` -> `WriteBuilder` (anonymous) -> `KafkaWrite` -> `KafkaBatchWrite` -> `KafkaBatchWriterFactory` -> `KafkaDataWriter`

#### KafkaTable (SupportsWrite)

**File**: `connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/KafkaSourceProvider.scala`

```scala
class KafkaTable(includeHeaders: Boolean) extends Table with SupportsRead with SupportsWrite {
  override def capabilities(): ju.Set[TableCapability] = {
    ju.EnumSet.of(BATCH_READ, BATCH_WRITE, MICRO_BATCH_READ,
      CONTINUOUS_READ, STREAMING_WRITE, ACCEPT_ANY_SCHEMA)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteBuilder with SupportsTruncate with SupportsStreamingUpdateAsAppend {
      private val inputSchema = info.schema()
      private val topic = Option(info.options.get("topic")).map(_.trim)
      private val producerParams = kafkaParamsForProducer(...)

      override def build(): Write = KafkaWrite(topic, producerParams, inputSchema)
      override def truncate(): WriteBuilder = this  // Kafka truncate is a no-op
    }
  }
}
```

#### KafkaWrite

**File**: `connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/KafkaWrite.scala`

```scala
case class KafkaWrite(
    topic: Option[String],
    producerParams: ju.Map[String, Object],
    schema: StructType) extends Write {

  override def description(): String = "Kafka"

  override def toBatch: BatchWrite = {
    new KafkaBatchWrite(topic, producerParams, schema)
  }

  override def toStreaming: StreamingWrite = {
    new KafkaStreamingWrite(topic, producerParams, schema)
  }
}
```

#### KafkaBatchWrite and KafkaBatchWriterFactory

**File**: `connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/KafkaBatchWrite.scala`

```scala
class KafkaBatchWrite(
    topic: Option[String],
    producerParams: ju.Map[String, Object],
    schema: StructType) extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): KafkaBatchWriterFactory =
    KafkaBatchWriterFactory(topic, producerParams, schema)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}
  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

case class KafkaBatchWriterFactory(
    topic: Option[String],
    producerParams: ju.Map[String, Object],
    schema: StructType) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new KafkaDataWriter(topic, producerParams, DataTypeUtils.toAttributes(schema))
  }
}
```

Kafka's `commit()` and `abort()` on `BatchWrite` are no-ops because Kafka writes are inherently
fire-and-forget at the batch level -- each record is independently sent to the Kafka broker.

#### KafkaDataWriter

**File**: `connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/KafkaDataWriter.scala`

```scala
case object KafkaDataWriterCommitMessage extends WriterCommitMessage

class KafkaDataWriter(
    targetTopic: Option[String],
    producerParams: ju.Map[String, Object],
    inputSchema: Seq[Attribute])
  extends KafkaRowWriter(inputSchema, targetTopic) with DataWriter[InternalRow] {

  private var producer: Option[CachedKafkaProducer] = None

  def write(row: InternalRow): Unit = {
    checkForErrors()
    if (producer.isEmpty) {
      producer = Some(InternalKafkaProducerPool.acquire(producerParams))
    }
    producer.foreach { p => sendRow(row, p.producer) }
  }

  def commit(): WriterCommitMessage = {
    checkForErrors()
    producer.foreach(_.producer.flush())  // Ensure all records are sent
    checkForErrors()
    KafkaDataWriterCommitMessage           // Dummy message
  }

  def abort(): Unit = {}

  def close(): Unit = {
    producer.foreach(InternalKafkaProducerPool.release)
    producer = None
  }
}
```

#### KafkaStreamingWrite

**File**: `connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/KafkaStreamingWrite.scala`

```scala
class KafkaStreamingWrite(
    topic: Option[String],
    producerParams: ju.Map[String, Object],
    schema: StructType) extends StreamingWrite {

  override def createStreamingWriterFactory(
      info: PhysicalWriteInfo): KafkaStreamWriterFactory =
    KafkaStreamWriterFactory(topic, producerParams, schema)

  override def useCommitCoordinator(): Boolean = false

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
}

case class KafkaStreamWriterFactory(
    topic: Option[String],
    producerParams: ju.Map[String, Object],
    schema: StructType) extends StreamingDataWriterFactory {

  override def createWriter(
      partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new KafkaDataWriter(topic, producerParams, DataTypeUtils.toAttributes(schema))
  }
}
```

### 12.2 File-Based Connectors (Parquet, JSON, CSV, ORC)

File-based connectors use the `FileWrite` / `FileBatchWrite` / `FileWriterFactory` framework.

#### FileWrite

**File**: `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileWrite.scala`

```scala
trait FileWrite extends Write {
  def paths: Seq[String]
  def formatName: String
  def supportsDataType: DataType => Boolean
  def info: LogicalWriteInfo

  override def description(): String = formatName

  override def toBatch: BatchWrite = {
    val sparkSession = SparkSession.active
    validateInputs(sparkSession.sessionState.conf)
    val path = new Path(paths.head)
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(...)
    val job = getJobInstance(hadoopConf, path)
    val committer = FileCommitProtocol.instantiate(
      sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = UUID.randomUUID().toString,
      outputPath = paths.head)

    committer.setupJob(job)
    new FileBatchWrite(job, description, committer)
  }

  /** Subclasses implement this to configure the output format. */
  def prepareWrite(
      sqlConf: SQLConf,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory
}
```

#### FileBatchWrite

**File**: `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileBatchWrite.scala`

```scala
class FileBatchWrite(
    job: Job,
    description: WriteJobDescription,
    committer: FileCommitProtocol) extends BatchWrite with Logging {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    FileWriterFactory(description, committer)

  // Disables the commit coordinator -- uses Hadoop FileCommitProtocol instead
  override def useCommitCoordinator(): Boolean = false

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val results = messages.map(_.asInstanceOf[WriteTaskResult])
    committer.commitJob(job, results.map(_.commitMsg))
    processStats(description.statsTrackers, results.map(_.summary.stats), duration)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    committer.abortJob(job)
  }
}
```

Key difference from Kafka: The file-based connector sets `useCommitCoordinator()` to `false`
because it relies on the Hadoop `FileCommitProtocol` for atomicity (renaming temp files on commit).

#### FileWriterFactory

**File**: `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileWriterFactory.scala`

```scala
case class FileWriterFactory(
    description: WriteJobDescription,
    committer: FileCommitProtocol) extends DataWriterFactory {

  override def createWriter(partitionId: Int, realTaskId: Long): DataWriter[InternalRow] = {
    val taskAttemptContext = createTaskAttemptContext(partitionId, realTaskId)
    committer.setupTask(taskAttemptContext)
    if (description.partitionColumns.isEmpty) {
      new SingleDirectoryDataWriter(description, taskAttemptContext, committer)
    } else {
      new DynamicPartitionDataSingleWriter(description, taskAttemptContext, committer)
    }
  }
}
```

---

## 13. Implementing a New V2 Write Data Source

This section provides a step-by-step guide for implementing a complete V2 write data source.

### Step 1: Implement `Table` with `SupportsWrite`

```java
public class MyTable implements Table, SupportsWrite {
    @Override
    public String name() { return "my_table"; }

    @Override
    public StructType schema() { return mySchema; }

    @Override
    public Set<TableCapability> capabilities() {
        return EnumSet.of(
            TableCapability.BATCH_WRITE,
            TableCapability.TRUNCATE,
            TableCapability.OVERWRITE_BY_FILTER
        );
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new MyWriteBuilder(info);
    }
}
```

### Step 2: Implement `WriteBuilder` with Capability Mixins

```java
public class MyWriteBuilder implements WriteBuilder, SupportsOverwriteV2 {
    private final LogicalWriteInfo info;
    private Predicate[] overwritePredicates = null;

    public MyWriteBuilder(LogicalWriteInfo info) {
        this.info = info;
    }

    @Override
    public WriteBuilder overwrite(Predicate[] predicates) {
        this.overwritePredicates = predicates;
        return this;
    }

    @Override
    public Write build() {
        return new MyWrite(info, overwritePredicates);
    }
}
```

### Step 3: Implement `Write`

```java
public class MyWrite implements Write {
    private final LogicalWriteInfo info;
    private final Predicate[] overwritePredicates;

    public MyWrite(LogicalWriteInfo info, Predicate[] overwritePredicates) {
        this.info = info;
        this.overwritePredicates = overwritePredicates;
    }

    @Override
    public String description() { return "MyDataSource"; }

    @Override
    public BatchWrite toBatch() {
        return new MyBatchWrite(info, overwritePredicates);
    }
}
```

### Step 4: Implement `BatchWrite`

```java
public class MyBatchWrite implements BatchWrite {
    private final LogicalWriteInfo info;
    private final Predicate[] overwritePredicates;

    public MyBatchWrite(LogicalWriteInfo info, Predicate[] overwritePredicates) {
        this.info = info;
        this.overwritePredicates = overwritePredicates;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalInfo) {
        // physicalInfo.numPartitions() tells you the parallelism
        return new MyDataWriterFactory(info.schema(), info.options());
    }

    @Override
    public boolean useCommitCoordinator() {
        return true;  // Recommended for most data sources
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        // Atomically publish all written data.
        // Use messages to identify which files/partitions to publish.
        // If overwritePredicates != null, delete matching data first.
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        // Clean up any partially written data.
        // messages may contain null entries.
    }
}
```

### Step 5: Implement `DataWriterFactory` (Must be Serializable)

```java
public class MyDataWriterFactory implements DataWriterFactory {
    private final StructType schema;
    private final CaseInsensitiveStringMap options;

    public MyDataWriterFactory(StructType schema, CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.options = options;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        // Create a writer for this partition/task combination.
        // Use taskId to create unique temp file names for speculative execution safety.
        return new MyDataWriter(schema, options, partitionId, taskId);
    }
}
```

### Step 6: Implement `DataWriter`

```java
public class MyDataWriter implements DataWriter<InternalRow> {
    private final List<InternalRow> buffer = new ArrayList<>();
    private final String tempPath;

    public MyDataWriter(StructType schema, CaseInsensitiveStringMap options,
                        int partitionId, long taskId) {
        // Use partitionId and taskId to create unique temp paths
        this.tempPath = "/tmp/my-ds/" + partitionId + "-" + taskId;
    }

    @Override
    public void write(InternalRow record) throws IOException {
        // IMPORTANT: Spark reuses the same InternalRow instance.
        // You MUST copy the data if you buffer it.
        buffer.add(record.copy());
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        // Flush buffered data to temp location.
        // Return a message with metadata about what was written.
        flushToTempFile(buffer, tempPath);
        return new MyCommitMessage(tempPath);
    }

    @Override
    public void abort() throws IOException {
        // Clean up temp files.
        deleteIfExists(tempPath);
    }

    @Override
    public void close() throws IOException {
        // Release resources (connections, file handles, etc.)
        buffer.clear();
    }
}
```

### Step 7: Implement `WriterCommitMessage`

```java
public class MyCommitMessage implements WriterCommitMessage {
    private final String tempPath;

    public MyCommitMessage(String tempPath) {
        this.tempPath = tempPath;
    }

    public String getTempPath() { return tempPath; }
}
```

### Common Pitfalls

1. **InternalRow reuse**: Spark reuses the same `InternalRow` object instance across calls to
   `write()`. Always call `record.copy()` before storing records in a buffer.

2. **Speculative execution**: Multiple task attempts may run for the same partition. Use `taskId`
   to create unique temporary paths. The commit coordinator ensures only one attempt commits.

3. **Null messages in abort**: When `abort(WriterCommitMessage[])` is called, the message array
   may contain null entries for tasks that never committed. Always null-check.

4. **Serialization**: `DataWriterFactory` and `WriterCommitMessage` must be `Serializable`.
   `DataWriter` does not need to be serializable (it runs on a single executor).

5. **Zero partitions**: Spark creates a dummy single-partition RDD when the input has zero
   partitions, so your writer should handle empty iterators gracefully.

6. **Commit coordinator**: For data sources that use their own commit protocol (e.g., Hadoop
   `FileCommitProtocol`), set `useCommitCoordinator()` to `false` to avoid conflicts.

---

## Source File Reference

| Interface / Class | File Path |
|---|---|
| `SupportsWrite` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/SupportsWrite.java` |
| `WriteBuilder` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/WriteBuilder.java` |
| `Write` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/Write.java` |
| `BatchWrite` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/BatchWrite.java` |
| `DataWriterFactory` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/DataWriterFactory.java` |
| `DataWriter<T>` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/DataWriter.java` |
| `WriterCommitMessage` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/WriterCommitMessage.java` |
| `LogicalWriteInfo` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/LogicalWriteInfo.java` |
| `PhysicalWriteInfo` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/PhysicalWriteInfo.java` |
| `SupportsTruncate` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/SupportsTruncate.java` |
| `SupportsOverwrite` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/SupportsOverwrite.java` |
| `SupportsOverwriteV2` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/SupportsOverwriteV2.java` |
| `SupportsDynamicOverwrite` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/SupportsDynamicOverwrite.java` |
| `RequiresDistributionAndOrdering` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/RequiresDistributionAndOrdering.java` |
| `WriteSummary` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/WriteSummary.java` |
| `MergeSummary` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/MergeSummary.java` |
| `DeltaWrite` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/DeltaWrite.java` |
| `DeltaWriter<T>` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/DeltaWriter.java` |
| `StreamingWrite` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/streaming/StreamingWrite.java` |
| `StreamingDataWriterFactory` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/streaming/StreamingDataWriterFactory.java` |
| `StagingTableCatalog` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/StagingTableCatalog.java` |
| `StagedTable` | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/StagedTable.java` |
| `DataFrameWriterV2` (API) | `sql/api/src/main/scala/org/apache/spark/sql/DataFrameWriterV2.scala` |
| `DataFrameWriterV2` (impl) | `sql/core/src/main/scala/org/apache/spark/sql/classic/DataFrameWriterV2.scala` |
| V2 write logical plans | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/v2Commands.scala` |
| Physical execution | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/WriteToDataSourceV2Exec.scala` |
| `FileWrite` | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileWrite.scala` |
| `FileBatchWrite` | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileBatchWrite.scala` |
| `FileWriterFactory` | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileWriterFactory.scala` |
| `KafkaWrite` | `connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/KafkaWrite.scala` |
| `KafkaBatchWrite` | `connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/KafkaBatchWrite.scala` |
| `KafkaDataWriter` | `connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/KafkaDataWriter.scala` |
| `KafkaStreamingWrite` | `connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/KafkaStreamingWrite.scala` |
| `KafkaTable` | `connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/KafkaSourceProvider.scala` |
