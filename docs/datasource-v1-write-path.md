# DataSourceV1 Write Path -- Developer Documentation

> Apache Spark 4.2.0-SNAPSHOT -- This document describes the internal architecture of the
> DataSource V1 write path. It is intended for developers who need to understand or extend the
> V1 write infrastructure, for example by implementing a custom file format or a custom
> `CreatableRelationProvider`.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Key Interfaces](#2-key-interfaces)
3. [Save Modes](#3-save-modes)
4. [File Writing Orchestration -- FileFormatWriter](#4-file-writing-orchestration----fileformatwriter)
5. [Per-Task Data Writers](#5-per-task-data-writers)
6. [Commit Protocol](#6-commit-protocol)
7. [Partitioning](#7-partitioning)
8. [Bucketing](#8-bucketing)
9. [DataFrameWriter Flow](#9-dataframewriter-flow)
10. [Format-Specific Examples](#10-format-specific-examples)

---

## 1. Overview

The DataSource V1 write path handles persisting DataFrame contents to external storage systems
through two distinct pipelines:

**Pipeline A -- Non-file data sources (e.g., JDBC):**

```
DataFrameWriter.save()
  -> DataSource.planForWriting()
    -> SaveIntoDataSourceCommand
      -> CreatableRelationProvider.createRelation(sqlContext, mode, parameters, data)
```

**Pipeline B -- File-based data sources (e.g., Parquet, CSV, JSON, ORC):**

```
DataFrameWriter.save()
  -> DataSource.planForWriting()
    -> InsertIntoHadoopFsRelationCommand
      -> FileFormatWriter.write()
        -> FileFormat.prepareWrite() => OutputWriterFactory
        -> FileCommitProtocol.setupJob()
        -> Per-partition Spark tasks:
             FileCommitProtocol.setupTask()
             FileFormatDataWriter.writeWithIterator()
               OutputWriter.write(row)  [per row]
             FileFormatDataWriter.commit()
               FileCommitProtocol.commitTask()
        -> FileCommitProtocol.commitJob()
```

There is also a third entry point for INSERT INTO on existing relations via
`InsertIntoDataSourceCommand`, which calls `InsertableRelation.insert()`.

### Source File Locations

| Component | File Path |
|-----------|-----------|
| CreatableRelationProvider, InsertableRelation | `sql/core/src/main/scala/org/apache/spark/sql/sources/interfaces.scala` |
| FileFormat, TextBasedFileFormat | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileFormat.scala` |
| OutputWriter, OutputWriterFactory | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/OutputWriter.scala` |
| FileFormatWriter | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileFormatWriter.scala` |
| FileFormatDataWriter hierarchy | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileFormatDataWriter.scala` |
| FileCommitProtocol | `core/src/main/scala/org/apache/spark/internal/io/FileCommitProtocol.scala` |
| HadoopMapReduceCommitProtocol | `core/src/main/scala/org/apache/spark/internal/io/HadoopMapReduceCommitProtocol.scala` |
| SaveIntoDataSourceCommand | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/SaveIntoDataSourceCommand.scala` |
| InsertIntoDataSourceCommand | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/InsertIntoDataSourceCommand.scala` |
| InsertIntoHadoopFsRelationCommand | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/InsertIntoHadoopFsRelationCommand.scala` |
| DataFrameWriter (classic) | `sql/core/src/main/scala/org/apache/spark/sql/classic/DataFrameWriter.scala` |
| V1WriteCommand, V1WritesUtils | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/V1Writes.scala` |
| DataSource.planForWriting | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSource.scala` |

---

## 2. Key Interfaces

### 2.1 CreatableRelationProvider

Defined in `org.apache.spark.sql.sources`. A non-file data source that can write a DataFrame must
implement this trait. The `createRelation` method receives the full DataFrame and is responsible
for handling save mode logic, writing data, and returning a `BaseRelation`.

```scala
@Stable
trait CreatableRelationProvider {
  /**
   * Saves a DataFrame to a destination (using data source-specific parameters)
   *
   * @param sqlContext SQLContext
   * @param mode specifies what happens when the destination already exists
   * @param parameters data source-specific parameters
   * @param data DataFrame to save (i.e. the rows after executing the query)
   * @return Relation with a known schema
   *
   * @since 1.3.0
   */
  def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation

  /**
   * Check if the relation supports the given data type.
   * @since 4.0.0
   */
  def supportsDataType(dt: DataType): Boolean = { /* default implementation */ }
}
```

The provider is fully responsible for:
- Checking whether the target already exists
- Implementing the requested `SaveMode` semantics (Append, Overwrite, ErrorIfExists, Ignore)
- Actually writing the data
- Returning a `BaseRelation` that represents the written result

### 2.2 InsertableRelation

Also defined in `org.apache.spark.sql.sources`. Used by `InsertIntoDataSourceCommand` for
INSERT INTO operations on existing relations.

```scala
@Stable
trait InsertableRelation {
  def insert(data: DataFrame, overwrite: Boolean): Unit
}
```

Key assumptions documented in the source:
1. The data (rows in the DataFrame) exactly matches the ordinal of fields in the relation schema.
2. The schema of this relation will not change even if the insert updates it.
3. Fields of the data provided are assumed nullable.

### 2.3 FileFormat.prepareWrite

The `FileFormat` trait defines the `prepareWrite` method, which is the file-format-specific hook
for driver-side job preparation. It returns an `OutputWriterFactory` that will be serialized to
executors to create per-task `OutputWriter` instances.

```scala
trait FileFormat {
  /**
   * Prepares a write job and returns an [[OutputWriterFactory]]. Client side job preparation can
   * be put here. For example, user defined output committer can be configured here
   * by setting the output committer class in the conf of
   * spark.sql.sources.outputCommitterClass.
   */
  def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory

  // ... other methods for reading, schema inference, etc.
}
```

The `dataSchema` parameter contains only data columns (partition columns are excluded). The `job`
parameter is a Hadoop `Job` object whose `Configuration` can be modified with format-specific
settings (e.g., compression codec, Parquet write support class).

### 2.4 OutputWriterFactory

Created on the driver by `FileFormat.prepareWrite()`, serialized to executors. The factory is
responsible for creating new `OutputWriter` instances and reporting file extensions.

```scala
abstract class OutputWriterFactory extends Serializable {

  /** Returns the file extension to be used when writing files out. */
  def getFileExtension(context: TaskAttemptContext): String

  /**
   * When writing to a [[HadoopFsRelation]], this method gets called by each task on executor
   * side to instantiate new [[OutputWriter]]s.
   *
   * @param path Path to write the file.
   * @param dataSchema Schema of the rows to be written. Partition columns are not included in
   *        the schema if the relation being written is partitioned.
   * @param context The Hadoop MapReduce task context.
   */
  def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter
}
```

### 2.5 OutputWriter

The per-file writer, instantiated on executors. Each `OutputWriter` corresponds to a single
output file. Subclasses must provide a zero-argument constructor.

```scala
abstract class OutputWriter {
  /**
   * Persists a single row. Invoked on the executor side. When writing to dynamically
   * partitioned tables, dynamic partition columns are not included in rows to be written.
   */
  def write(row: InternalRow): Unit

  /**
   * Closes the [[OutputWriter]]. Invoked on the executor side after all rows are persisted,
   * before the task output is committed.
   */
  def close(): Unit

  /**
   * The file path to write. Invoked on the executor side.
   */
  def path(): String
}
```

---

## 3. Save Modes

The `SaveMode` enum is defined in `sql/api/src/main/java/org/apache/spark/sql/SaveMode.java`:

```java
@Stable
public enum SaveMode {
  /** Append to existing data. */
  Append,
  /** Overwrite existing data. */
  Overwrite,
  /** Throw an exception if data already exists. */
  ErrorIfExists,
  /** Do nothing if data already exists. */
  Ignore
}
```

### How Each Mode is Handled

**For non-file data sources (CreatableRelationProvider):** The save mode is passed directly to
`createRelation()`. The provider is responsible for implementing the semantics. For example, see
`JdbcRelationProvider.createRelation()`:

| Mode | JDBC behavior |
|------|------|
| `Overwrite` | If `truncate` option is true and cascading truncate is false, truncates then inserts. Otherwise drops and recreates the table, then inserts. |
| `Append` | Inserts rows into the existing table. |
| `ErrorIfExists` | Throws `tableOrViewAlreadyExistsError` if table exists. |
| `Ignore` | If the table exists, does nothing. |

**For file data sources (InsertIntoHadoopFsRelationCommand):** Save mode logic is implemented in
`InsertIntoHadoopFsRelationCommand.run()`:

| Mode | Behavior |
|------|------|
| `Append` | Always proceeds with insertion. |
| `Overwrite` | Deletes matching partitions (or entire output path) before writing. With `dynamicPartitionOverwrite` enabled, writes to a staging directory first and renames on job commit. |
| `ErrorIfExists` | Throws `outputPathAlreadyExistsError` if the path already exists. |
| `Ignore` | Skips writing entirely if the path already exists. |

The `dynamicPartitionOverwrite` behavior is controlled by the `spark.sql.sources.partitionOverwriteMode`
configuration (values: `STATIC` or `DYNAMIC`). When set to `DYNAMIC`, only the partitions that are
actually written are replaced, rather than the entire table.

---

## 4. File Writing Orchestration -- FileFormatWriter

`FileFormatWriter` (in `sql/core/.../execution/datasources/FileFormatWriter.scala`) is the central
orchestrator for writing data to file-based data sources. Its `write()` method coordinates the
entire write lifecycle.

### 4.1 FileFormatWriter.write() Signature

```scala
object FileFormatWriter extends Logging {

  case class OutputSpec(
      outputPath: String,
      customPartitionLocations: Map[TablePartitionSpec, String],
      outputColumns: Seq[Attribute])

  case class ConcurrentOutputWriterSpec(
      maxWriters: Int,
      createSorter: () => UnsafeExternalRowSorter)

  def write(
      sparkSession: SparkSession,
      plan: SparkPlan,
      fileFormat: FileFormat,
      committer: FileCommitProtocol,
      outputSpec: OutputSpec,
      hadoopConf: Configuration,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      statsTrackers: Seq[WriteJobStatsTracker],
      options: Map[String, String],
      numStaticPartitionCols: Int = 0): Set[String] = { ... }
}
```

### 4.2 Write Flow (Step by Step)

1. **Job setup** -- Creates a Hadoop `Job`, sets output key/value classes, and configures the
   output path.

2. **Schema preparation** -- Separates output columns into data columns and partition columns.
   Computes `WriterBucketSpec` and sort columns from the `BucketSpec`.

3. **prepareWrite** -- Calls `fileFormat.prepareWrite(sparkSession, job, options, dataSchema)` to
   get the `OutputWriterFactory`. This has side effects: it configures the Hadoop `Job`.

4. **WriteJobDescription** -- Constructs a `WriteJobDescription` containing all information tasks
   need:
   ```scala
   class WriteJobDescription(
       val uuid: String,
       val serializableHadoopConf: SerializableConfiguration,
       val outputWriterFactory: OutputWriterFactory,
       val allColumns: Seq[Attribute],
       val dataColumns: Seq[Attribute],
       val partitionColumns: Seq[Attribute],
       val bucketSpec: Option[WriterBucketSpec],
       val path: String,
       val customPartitionLocations: Map[TablePartitionSpec, String],
       val maxRecordsPerFile: Long,
       val timeZoneId: String,
       val statsTrackers: Seq[WriteJobStatsTracker])
     extends Serializable
   ```

5. **Sort planning** -- Determines the required ordering: dynamic partition columns, then
   bucket ID expression, then sort columns. If the physical plan's output ordering already
   satisfies this, no additional sort is needed. If `spark.sql.maxConcurrentOutputFileWriters > 0`
   and there are no sort columns, concurrent writers are used instead of sorting.

6. **Commit protocol setup** -- Calls `committer.setupJob(job)`.

7. **Task execution** -- Runs tasks via `sparkSession.sparkContext.runJob()`. Each task calls
   `executeTask()` which:
   - Creates Hadoop `TaskAttemptContext`
   - Calls `committer.setupTask(taskAttemptContext)`
   - Selects the appropriate `FileFormatDataWriter` subclass
   - Calls `dataWriter.writeWithIterator(iterator)` then `dataWriter.commit()`
   - On error, calls `dataWriter.abort()` (which invokes `committer.abortTask()`)

8. **Job commit** -- After all tasks succeed, calls `committer.commitJob(job, commitMsgs)`.
   On failure, calls `committer.abortJob(job)`.

9. **Stats processing** -- Calls `processStats()` on all registered `WriteJobStatsTracker`
   instances.

10. **Returns** -- The set of updated partition paths.

### 4.3 Task Writer Selection Logic

In `FileFormatWriter.executeTask()`, the writer is chosen based on the task state:

```scala
dataWriter =
  if (sparkPartitionId != 0 && !iterator.hasNext) {
    // Empty non-first partition: use no-op writer
    new EmptyDirectoryDataWriter(description, taskAttemptContext, committer)
  } else if (description.partitionColumns.isEmpty && description.bucketSpec.isEmpty) {
    // No partitioning or bucketing: single directory writer
    new SingleDirectoryDataWriter(description, taskAttemptContext, committer)
  } else {
    concurrentOutputWriterSpec match {
      case Some(spec) =>
        // Dynamic partitioning with concurrent writers
        new DynamicPartitionDataConcurrentWriter(
          description, taskAttemptContext, committer, spec)
      case _ =>
        // Dynamic partitioning with single writer (data must be pre-sorted)
        new DynamicPartitionDataSingleWriter(description, taskAttemptContext, committer)
    }
  }
```

---

## 5. Per-Task Data Writers

The `FileFormatDataWriter` hierarchy is defined in
`sql/core/.../execution/datasources/FileFormatDataWriter.scala`.

### 5.1 FileFormatDataWriter (abstract base)

```scala
abstract class FileFormatDataWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol,
    customMetrics: Map[String, SQLMetric]) extends DataWriter[InternalRow] {

  protected val MAX_FILE_COUNTER: Int = 1000 * 1000
  protected val updatedPartitions: mutable.Set[String] = mutable.Set[String]()
  protected var currentWriter: OutputWriter = _
  protected val statsTrackers: Seq[WriteTaskStatsTracker] = ...

  def write(record: InternalRow): Unit        // abstract -- subclasses implement
  def writeWithIterator(iterator: Iterator[InternalRow]): Unit
  final override def commit(): WriteTaskResult // releases resources, calls commitTask
  final def abort(): Unit                      // releases resources, calls abortTask
}
```

The `commit()` method:
1. Calls `releaseResources()` (which closes the current `OutputWriter`)
2. Calls `committer.commitTask(taskAttemptContext)` to get a `TaskCommitMessage`
3. Returns a `WriteTaskResult(taskCommitMessage, executedWriteSummary)`

### 5.2 EmptyDirectoryDataWriter

Used for empty partitions (non-first partition with no data). Writes nothing.

```scala
class EmptyDirectoryDataWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol,
    customMetrics: Map[String, SQLMetric] = Map.empty
) extends FileFormatDataWriter(...) {
  override def write(record: InternalRow): Unit = {}
}
```

The first partition (ID 0) is never assigned this writer, even if empty, so that metadata-only
formats like Parquet can still write their metadata.

### 5.3 SingleDirectoryDataWriter

Used when there is no partitioning and no bucketing. All rows go to a single directory, with
optional file splitting based on `maxRecordsPerFile`.

```scala
class SingleDirectoryDataWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol,
    customMetrics: Map[String, SQLMetric] = Map.empty)
  extends FileFormatDataWriter(...) {

  // Opens the first output file immediately on construction
  newOutputWriter()

  override def write(record: InternalRow): Unit = {
    if (description.maxRecordsPerFile > 0 && recordsInFile >= description.maxRecordsPerFile) {
      fileCounter += 1
      assert(fileCounter < MAX_FILE_COUNTER, ...)
      newOutputWriter()
    }
    currentWriter.write(record)
    statsTrackers.foreach(_.newRow(currentWriter.path(), record))
    recordsInFile += 1
  }
}
```

File naming uses `FileNameSpec("", f"-c$fileCounter%03d" + ext)`, producing names like
`part-00000-<uuid>-c000.snappy.parquet`.

### 5.4 BaseDynamicPartitionDataWriter

Abstract base for writers that handle partitioned and/or bucketed output. Provides:

- `getPartitionValues: InternalRow => UnsafeRow` -- Extracts partition column values
- `getPartitionPath: InternalRow => String` -- Builds `col1=val1/col2=val2/...` path strings
- `getBucketId: InternalRow => Int` -- Computes bucket ID from the bucket ID expression
- `getOutputRow` -- Projects out only data columns (excludes partition columns)
- `renewCurrentWriter(partitionValues, bucketId, closeCurrentWriter)` -- Opens a new file

The `renewCurrentWriter` method handles:
- Computing the partition subdirectory path
- Looking up custom partition locations
- Generating the file name with bucket ID prefix/suffix
- Calling `committer.newTaskTempFile()` or `committer.newTaskTempFileAbsPath()`
- Creating a new `OutputWriter` via `outputWriterFactory.newInstance()`

### 5.5 DynamicPartitionDataSingleWriter

Requires input data to be **sorted** by partition columns and bucket ID. Opens one file at a time.
When it detects a new partition/bucket combination, it closes the current writer and opens a new one.

```scala
class DynamicPartitionDataSingleWriter(...)
  extends BaseDynamicPartitionDataWriter(...) {

  override def write(record: InternalRow): Unit = {
    val nextPartitionValues = if (isPartitioned) Some(getPartitionValues(record)) else None
    val nextBucketId = if (isBucketed) Some(getBucketId(record)) else None

    if (currentPartitionValues != nextPartitionValues || currentBucketId != nextBucketId) {
      // New partition or bucket -- close old file, open new one
      currentPartitionValues = Some(nextPartitionValues.get.copy())
      fileCounter = 0
      renewCurrentWriter(currentPartitionValues, currentBucketId, closeCurrentWriter = true)
    } else if (description.maxRecordsPerFile > 0 &&
        recordsInFile >= description.maxRecordsPerFile) {
      renewCurrentWriterIfTooManyRecords(currentPartitionValues, currentBucketId)
    }
    writeRecord(record)
  }
}
```

### 5.6 DynamicPartitionDataConcurrentWriter

Handles dynamic partitions **without requiring pre-sorted input**. This is activated when
`spark.sql.maxConcurrentOutputFileWriters > 0` and there are no sort columns.

The process has two phases:

**Phase 1 -- Concurrent writing:**
Maintains a `HashMap[WriterIndex, WriterStatus]` of open writers. Each record is routed to
the appropriate writer based on its partition/bucket values. Writers remain open for reuse.

**Phase 2 -- Fallback to sort-based writing:**
When the number of concurrent writers reaches `maxWriters`, all remaining rows are sorted and
processed by a single writer that eagerly closes files when the partition/bucket changes.

```scala
class DynamicPartitionDataConcurrentWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol,
    concurrentOutputWriterSpec: ConcurrentOutputWriterSpec,
    customMetrics: Map[String, SQLMetric] = Map.empty)
  extends BaseDynamicPartitionDataWriter(...) with Logging {

  override def writeWithIterator(iterator: Iterator[InternalRow]): Unit = {
    // Phase 1: concurrent writing until max writers reached
    while (iterator.hasNext && !sorted) {
      writeWithMetrics(iterator.next(), count)
      count += 1
    }
    // Phase 2: sort remaining rows and write sequentially
    if (iterator.hasNext) {
      val sorter = concurrentOutputWriterSpec.createSorter()
      val sortIterator = sorter.sort(iterator.asInstanceOf[Iterator[UnsafeRow]])
      while (sortIterator.hasNext) {
        writeWithMetrics(sortIterator.next(), count)
        count += 1
      }
    }
  }
}
```

---

## 6. Commit Protocol

### 6.1 FileCommitProtocol (abstract)

Defined in `core/src/main/scala/org/apache/spark/internal/io/FileCommitProtocol.scala`. This
is the contract for atomic commit of write output.

```scala
@Unstable
abstract class FileCommitProtocol extends Logging {

  /** Driver: set up the overall job. */
  def setupJob(jobContext: JobContext): Unit

  /** Driver: commit all task outputs after all tasks succeed. */
  def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit

  /** Driver: abort the job after failure. */
  def abortJob(jobContext: JobContext): Unit

  /** Executor: set up a task before writing. */
  def setupTask(taskContext: TaskAttemptContext): Unit

  /** Executor: request a new temp file path for output. */
  def newTaskTempFile(
      taskContext: TaskAttemptContext,
      dir: Option[String],
      spec: FileNameSpec): String

  /** Executor: request a temp file at an absolute path. */
  def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext,
      absoluteDir: String,
      spec: FileNameSpec): String

  /** Executor: commit this task's output. */
  def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage

  /** Executor: abort this task's output after failure. */
  def abortTask(taskContext: TaskAttemptContext): Unit

  /** Driver: called after each task commits. */
  def onTaskCommit(taskCommit: TaskCommitMessage): Unit = {}

  /** Delete a file as part of job commit. */
  def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean = {
    fs.delete(path, recursive)
  }
}
```

### 6.2 Call Sequence

The proper call sequence, as documented in the source:

```
1. Driver: setupJob()
2. For each task on executor:
   a. setupTask()
   b. newTaskTempFile() / newTaskTempFileAbsPath()  [possibly multiple times]
   c. commitTask() on success  OR  abortTask() on failure
3. Driver: commitJob() on success  OR  abortJob() on failure
```

### 6.3 FileNameSpec

```scala
final case class FileNameSpec(prefix: String, suffix: String)
```

A full output file path consists of:
1. The base path
2. A subdirectory for partitioning (the `dir` parameter)
3. A file prefix (from `FileNameSpec.prefix`), often used for bucket IDs
4. A unique job/task identifier (added by the commit protocol)
5. A suffix (from `FileNameSpec.suffix`) containing bucket ID string and file extension

### 6.4 HadoopMapReduceCommitProtocol (default implementation)

Defined in `core/src/main/scala/org/apache/spark/internal/io/HadoopMapReduceCommitProtocol.scala`.
This is Spark's default commit protocol backed by Hadoop's `OutputCommitter`.

Key behaviors:

**File naming:**
```scala
protected def getFilename(taskContext: TaskAttemptContext, spec: FileNameSpec): String = {
  val split = taskContext.getTaskAttemptID.getTaskID.getId
  val basename = taskContext.getConfiguration.get("mapreduce.output.basename", "part")
  f"${spec.prefix}$basename-$split%05d-$jobId${spec.suffix}"
}
```

This produces file names like: `part-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-c000.snappy.parquet`

**Dynamic partition overwrite:** When enabled, writes to a staging directory
(`.spark-staging-{jobId}`). During `commitJob()`, staging partition directories are renamed
to their final locations, replacing only the partitions that were actually written.

**Absolute path files:** Tracked in `addedAbsPathFiles` (mapping from temp path to final path).
During `commitJob()`, these are moved from the staging directory to their absolute final locations.

### 6.5 Instantiation

```scala
object FileCommitProtocol {
  def instantiate(
      className: String,
      jobId: String,
      outputPath: String,
      dynamicPartitionOverwrite: Boolean = false): FileCommitProtocol
}
```

The protocol class is configured via `spark.sql.sources.commitProtocolClass` (defaults to
`HadoopMapReduceCommitProtocol`). The instantiation attempts a 3-argument constructor
`(jobId, path, dynamicPartitionOverwrite)`, falling back to a 2-argument constructor
`(jobId, path)`.

---

## 7. Partitioning

### 7.1 Dynamic Partition Writing

When a write operation involves partition columns, the data must be organized into
partition-specific subdirectories following the Hive-style `col=value` convention.

The `BaseDynamicPartitionDataWriter` handles this via a partition path expression:

```scala
private lazy val partitionPathExpression: Expression = Concat(
  description.partitionColumns.zipWithIndex.flatMap { case (c, i) =>
    val partitionName = ScalaUDF(
      ExternalCatalogUtils.getPartitionPathString _,
      StringType,
      Seq(Literal(c.name), Cast(c, StringType, Option(description.timeZoneId))))
    if (i == 0) Seq(partitionName) else Seq(Literal(Path.SEPARATOR), partitionName)
  })
```

This produces paths like `year=2024/month=01/day=15`.

### 7.2 Static vs. Dynamic Partitions

`InsertIntoHadoopFsRelationCommand` supports both static and dynamic partitions:

- **Static partitions** (`staticPartitions: TablePartitionSpec`): Partition columns with
  pre-determined values, set at the command level.
- **Dynamic partitions**: Partition columns whose values are determined per-row at runtime.

Static partition columns must appear before dynamic ones. The required sort ordering only covers
dynamic partition columns (static partitions have a single value and need no sorting).

### 7.3 Partition Overwrite Modes

Controlled by `spark.sql.sources.partitionOverwriteMode`:

- **STATIC** (default): On overwrite, deletes the entire output path (or the static partition
  prefix) before writing.
- **DYNAMIC**: Only replaces partitions that are actually written. Uses a staging directory.
  Files are written to `.spark-staging-{jobId}/` and renamed to final locations during job commit.

### 7.4 Custom Partition Locations

When catalog-managed partitions have custom locations (not the default Hive-style path), the
write command tracks these via `customPartitionLocations: Map[TablePartitionSpec, String]`. The
data writers use `committer.newTaskTempFileAbsPath()` for these partitions instead of the
normal `newTaskTempFile()`.

### 7.5 maxRecordsPerFile

Configured via the `maxRecordsPerFile` option or `spark.sql.files.maxRecordsPerFile`. When the
number of records written to a single file exceeds this threshold, a new file is created
(incrementing `fileCounter`). The maximum file counter is capped at `MAX_FILE_COUNTER = 1,000,000`.

### 7.6 Empty Partition Columns to Null

The `V1WritesUtils.convertEmptyToNull()` function wraps nullable `StringType` partition columns
with `Empty2Null` to convert empty strings to null values. This ensures that Hive-compatible
partition directories use `__HIVE_DEFAULT_PARTITION__` for null values instead of empty strings.

---

## 8. Bucketing

### 8.1 WriterBucketSpec

```scala
case class WriterBucketSpec(
  bucketIdExpression: Expression,
  bucketFileNamePrefix: Int => String)
```

Created by `V1WritesUtils.getWriterBucketSpec()`, which supports two bucketing modes:

**Spark bucketing:**
- Uses `HashPartitioning.partitionIdExpression` for bucket ID computation
- Empty file name prefix (bucket ID encoded in suffix)
- Guarantees data distribution matches shuffle partitioning

**Hive-compatible bucketing:**
- Uses `Pmod(BitwiseAnd(HiveHash(bucketColumns), Literal(Int.MaxValue)), Literal(numBuckets))`
- File name prefix: `f"$bucketId%05d_0_"` (e.g., `00002_0_`)
- Enabled by option `spark.sql.sources.bucketing.hiveCompatibleBucketWrite=true`

### 8.2 Bucket ID in File Names

Bucket information is encoded in the output file name. The `BaseDynamicPartitionDataWriter`
builds the file name spec as follows:

```scala
val bucketIdStr = bucketId.map(BucketingUtils.bucketIdToString).getOrElse("")
val prefix = bucketId match {
  case Some(id) => description.bucketSpec.get.bucketFileNamePrefix(id)
  case _ => ""
}
val suffix = f"$bucketIdStr.c$fileCounter%03d" +
  description.outputWriterFactory.getFileExtension(taskAttemptContext)
val fileNameSpec = FileNameSpec(prefix, suffix)
```

### 8.3 Sort Columns

When a `BucketSpec` includes sort columns, the data within each bucket is sorted by those
columns. The sort ordering is: dynamic partition columns, then bucket ID, then sort columns.

When sort columns are specified, concurrent writers are disabled (the data must be pre-sorted).

---

## 9. DataFrameWriter Flow

The user-facing entry point is `DataFrameWriter` (defined in
`sql/core/.../classic/DataFrameWriter.scala`).

### 9.1 save() and save(path)

```scala
def save(path: String): Unit = {
  runCommand(df.sparkSession) {
    saveCommand(Some(path))
  }
}

def save(): Unit = {
  runCommand(df.sparkSession) {
    saveCommand(None)
  }
}
```

### 9.2 saveCommand -- V1 vs V2 Routing

`saveCommand` first checks if the source has a V2 implementation:

```scala
private[sql] def saveCommand(path: Option[String]): LogicalPlan = {
  // ...
  val maybeV2Provider = lookupV2Provider()
  if (maybeV2Provider.isDefined) {
    // V2 path (AppendData, OverwriteByExpression, CreateTableAsSelect, etc.)
    // Falls back to V1 if the V2 provider doesn't support BATCH_WRITE
  } else {
    saveToV1SourceCommand(path)
  }
}
```

Note: File source V2 write path is currently disabled (`FileDataSourceV2` returns `None`):
```scala
private def lookupV2Provider(): Option[TableProvider] = {
  DataSource.lookupDataSourceV2(source, df.sparkSession.sessionState.conf) match {
    case Some(_: FileDataSourceV2) => None  // TODO(SPARK-28396): File source v2 write is broken
    case other => other
  }
}
```

### 9.3 V1 Source Command Path

```scala
private def saveToV1SourceCommand(path: Option[String]): LogicalPlan = {
  // Encode partitioning columns into options
  partitioningColumns.foreach { columns =>
    extraOptions = extraOptions + (
      DataSourceUtils.PARTITIONING_COLUMNS_KEY ->
      DataSourceUtils.encodePartitioningColumns(columns))
  }
  val optionsWithPath = getOptionsWithPath(path)

  DataSource(
    sparkSession = df.sparkSession,
    className = source,
    partitionColumns = partitioningColumns.getOrElse(Nil),
    options = optionsWithPath.originalMap
  ).planForWriting(curmode, df.logicalPlan)
}
```

### 9.4 DataSource.planForWriting

This is the branching point between file and non-file V1 data sources:

```scala
def planForWriting(mode: SaveMode, data: LogicalPlan): LogicalPlan = {
  providingInstance() match {
    case dataSource: CreatableRelationProvider =>
      // Non-file data source: wrap in SaveIntoDataSourceCommand
      SaveIntoDataSourceCommand(data, dataSource, caseInsensitiveOptions, mode)
    case format: FileFormat =>
      // File data source: create InsertIntoHadoopFsRelationCommand
      planForWritingFileFormat(format, mode, data)
    case _ => throw ...
  }
}
```

### 9.5 SaveIntoDataSourceCommand

For non-file data sources, `SaveIntoDataSourceCommand.run()` simply delegates to the provider:

```scala
case class SaveIntoDataSourceCommand(
    query: LogicalPlan,
    dataSource: CreatableRelationProvider,
    options: Map[String, String],
    mode: SaveMode) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val relation = dataSource.createRelation(
      sparkSession.sqlContext, mode, options, Dataset.ofRows(sparkSession, query))
    // Recache if applicable
    sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, logicalRelation)
    Seq.empty[Row]
  }
}
```

### 9.6 InsertIntoDataSourceCommand

For INSERT INTO on existing relations with `InsertableRelation`:

```scala
case class InsertIntoDataSourceCommand(
    logicalRelation: LogicalRelation,
    query: LogicalPlan,
    overwrite: Boolean) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val relation = logicalRelation.relation.asInstanceOf[InsertableRelation]
    val data = Dataset.ofRows(sparkSession, query)
    relation.insert(data, overwrite)
    sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, logicalRelation)
    Seq.empty[Row]
  }
}
```

### 9.7 V1Writes Optimizer Rule

The `V1Writes` optimizer rule (in `V1Writes.scala`) handles planned write mode. When
`spark.sql.optimizer.plannedWrite.enabled` is true, it:

1. Wraps the query child of `V1WriteCommand` in a `WriteFiles` logical node
2. Applies `Empty2Null` transformations on nullable string partition columns
3. Adds a `Sort` node if the required ordering is not already satisfied

This ensures the physical sort is planned as part of the query plan rather than being added
at execution time in `FileFormatWriter`.

---

## 10. Format-Specific Examples

### 10.1 Parquet Write

**prepareWrite implementation** (in `ParquetFileFormat` / `ParquetUtils`):

```scala
// ParquetFileFormat.scala
override def prepareWrite(
    sparkSession: SparkSession,
    job: Job,
    options: Map[String, String],
    dataSchema: StructType): OutputWriterFactory = {
  val sqlConf = getSqlConf(sparkSession)
  val parquetOptions = new ParquetOptions(options, sqlConf)
  ParquetUtils.prepareWrite(sqlConf, job, dataSchema, parquetOptions)
}
```

`ParquetUtils.prepareWrite()` configures:
- Output committer class (default: `ParquetOutputCommitter`)
- Output format class (`ParquetOutputFormat`)
- Write support class (`ParquetWriteSupport`)
- Schema metadata, timestamp type, legacy format flags
- Compression codec

**ParquetOutputWriter:**

```scala
class ParquetOutputWriter(val path: String, context: TaskAttemptContext)
  extends OutputWriter {

  private val recordWriter: RecordWriter[Void, InternalRow] = {
    new ParquetOutputFormat[InternalRow]() {
      override def getDefaultWorkFile(
          context: TaskAttemptContext, extension: String): Path = {
        new Path(path)
      }
    }.getRecordWriter(context)
  }

  override def write(row: InternalRow): Unit = recordWriter.write(null, row)

  override def close(): Unit = recordWriter.close(context)
}
```

The `ParquetOutputWriter` overrides `getDefaultWorkFile` to use the path provided by the
commit protocol, enabling dynamic partitioning and appending.

### 10.2 CSV Write

**prepareWrite implementation** (in `CSVFileFormat`):

```scala
override def prepareWrite(
    sparkSession: SparkSession,
    job: Job,
    options: Map[String, String],
    dataSchema: StructType): OutputWriterFactory = {
  val parsedOptions = getCsvOptions(sparkSession, options)
  parsedOptions.compressionCodec.foreach { codec =>
    CompressionCodecs.setCodecConfiguration(job.getConfiguration, codec)
  }

  new OutputWriterFactory {
    override def newInstance(
        path: String,
        dataSchema: StructType,
        context: TaskAttemptContext): OutputWriter = {
      new CsvOutputWriter(path, dataSchema, context, parsedOptions)
    }

    override def getFileExtension(context: TaskAttemptContext): String = {
      "." + parsedOptions.extension + CodecStreams.getCompressionExtension(context)
    }
  }
}
```

Key details:
- The `CSVOptions` are parsed from user options and session config on the driver
- The `OutputWriterFactory` is an anonymous class that captures `parsedOptions` for serialization
- File extension includes the format extension (e.g., `.csv`) plus any compression extension

### 10.3 JSON Write

**prepareWrite implementation** (in `JsonFileFormat`):

```scala
override def prepareWrite(
    sparkSession: SparkSession,
    job: Job,
    options: Map[String, String],
    dataSchema: StructType): OutputWriterFactory = {
  val parsedOptions = getJsonOptions(sparkSession, options, inRead = false)
  parsedOptions.compressionCodec.foreach { codec =>
    CompressionCodecs.setCodecConfiguration(job.getConfiguration, codec)
  }

  new OutputWriterFactory {
    override def newInstance(
        path: String,
        dataSchema: StructType,
        context: TaskAttemptContext): OutputWriter = {
      new JsonOutputWriter(path, parsedOptions, dataSchema, context)
    }

    override def getFileExtension(context: TaskAttemptContext): String = {
      ".json" + CodecStreams.getCompressionExtension(context)
    }
  }
}
```

Similar to CSV: parses options on the driver, captures them in the factory for executor-side use.

### 10.4 JDBC Write

JDBC uses `CreatableRelationProvider` (not `FileFormat`), so the entire write is handled in
`createRelation()`:

```scala
class JdbcRelationProvider extends CreatableRelationProvider
  with RelationProvider with DataSourceRegister {

  override def shortName(): String = "jdbc"

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      df: DataFrame): BaseRelation = {
    val options = new JdbcOptionsInWrite(parameters)
    val conn = dialect.createConnectionFactory(options)(-1)
    try {
      val tableExists = JdbcUtils.tableExists(conn, options)
      if (tableExists) {
        mode match {
          case SaveMode.Overwrite =>
            if (options.isTruncate &&
                isCascadingTruncateTable(options.url) == Some(false)) {
              truncateTable(conn, options)
              val tableSchema = JdbcUtils.getSchemaOption(conn, options)
              saveTable(df, tableSchema, isCaseSensitive, options)
            } else {
              dropTable(conn, options.table, options)
              createTable(conn, options.table, df.schema, isCaseSensitive, options)
              saveTable(df, Some(df.schema), isCaseSensitive, options)
            }
          case SaveMode.Append =>
            val tableSchema = JdbcUtils.getSchemaOption(conn, options)
            saveTable(df, tableSchema, isCaseSensitive, options)
          case SaveMode.ErrorIfExists =>
            throw QueryCompilationErrors.tableOrViewAlreadyExistsError(options.table)
          case SaveMode.Ignore =>
            // Do nothing
        }
      } else {
        createTable(conn, options.table, df.schema, isCaseSensitive, options)
        saveTable(df, Some(df.schema), isCaseSensitive, options)
      }
    } finally {
      conn.close()
    }
    createRelation(sqlContext, parameters)
  }
}
```

JDBC handles save modes entirely within the provider. Note that the provider handles the
"table does not exist" case uniformly: create the table then insert, regardless of mode.

---

## Appendix: Implementing a Custom V1 File Format Write Path

To implement write support for a new file format, you need to:

1. **Extend `FileFormat`** (or `TextBasedFileFormat` for text-based formats):
   ```scala
   class MyFileFormat extends FileFormat {
     override def prepareWrite(
         sparkSession: SparkSession,
         job: Job,
         options: Map[String, String],
         dataSchema: StructType): OutputWriterFactory = {
       // Configure job (compression, output format, etc.)
       // Return an OutputWriterFactory
       new OutputWriterFactory {
         override def getFileExtension(context: TaskAttemptContext): String =
           ".myformat" + CodecStreams.getCompressionExtension(context)

         override def newInstance(
             path: String,
             dataSchema: StructType,
             context: TaskAttemptContext): OutputWriter = {
           new MyOutputWriter(path, dataSchema, context)
         }
       }
     }

     override def inferSchema(...): Option[StructType] = { ... }
   }
   ```

2. **Extend `OutputWriter`:**
   ```scala
   class MyOutputWriter(
       val path: String,
       dataSchema: StructType,
       context: TaskAttemptContext) extends OutputWriter {

     // Initialize your format-specific writer (e.g., open file, create encoder)
     private val writer = ...

     override def write(row: InternalRow): Unit = {
       // Convert InternalRow to your format and write
       writer.write(row)
     }

     override def close(): Unit = {
       writer.close()
     }
   }
   ```

3. **Optionally implement `DataSourceRegister`** for a short name alias:
   ```scala
   class MyFileFormat extends FileFormat with DataSourceRegister {
     override def shortName(): String = "myformat"
     // ...
   }
   ```

4. **Register the format** by adding the fully qualified class name to
   `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`.

To implement write support as a non-file data source, implement `CreatableRelationProvider`
and optionally `InsertableRelation` on your `BaseRelation`.
