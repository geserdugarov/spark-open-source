# Apache Spark DataSourceV1 Read Path

Developer documentation for the DataSource V1 read path in Apache Spark 4.2.0-SNAPSHOT.
This guide covers the full pipeline from the public `DataFrameReader` API down through provider
resolution, relation creation, query planning, and RDD execution.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Key Interfaces](#2-key-interfaces)
   - [DataSourceRegister](#datasourceregister)
   - [RelationProvider / SchemaRelationProvider](#relationprovider--schemarelationprovider)
   - [BaseRelation](#baserelation)
   - [Scan Traits: TableScan / PrunedScan / PrunedFilteredScan / CatalystScan](#scan-traits)
   - [FileFormat](#fileformat)
   - [HadoopFsRelation](#hadoopfsrelation)
3. [Schema Inference](#3-schema-inference)
4. [Filter Pushdown and Partition Pruning](#4-filter-pushdown-and-partition-pruning)
5. [Execution Model](#5-execution-model)
6. [DataFrameReader Flow](#6-dataframereader-flow)
7. [Format Examples](#7-format-examples)

---

## 1. Overview

The DataSource V1 read path follows a four-stage pipeline:

```
DataFrameReader.load()
  --> ResolveDataSource (analyzer rule)
    --> DataSource.resolveRelation()           // provider + relation creation
      --> LogicalRelation(BaseRelation)        // logical plan node
        --> DataSourceStrategy / FileSourceStrategy   // physical planning
          --> RowDataSourceScanExec / FileSourceScanExec  // physical plan nodes
            --> RDD[InternalRow] / FileScanRDD            // execution
```

**Stage 1 -- Provider Resolution.** The `DataSource` class looks up a provider class by short
name or fully-qualified class name using Java `ServiceLoader` (via `DataSourceRegister.shortName`)
and reflection fallback.

**Stage 2 -- Relation Creation.** The provider is instantiated and, depending on its type, one
of these code paths runs:

- `RelationProvider.createRelation(sqlContext, parameters)` -- the provider infers schema itself.
- `SchemaRelationProvider.createRelation(sqlContext, parameters, schema)` -- the caller supplies
  a schema.
- `FileFormat` -- the `DataSource` constructs a `HadoopFsRelation` wrapping the file format,
  partitioning information, and a `FileIndex`.

The resulting `BaseRelation` is wrapped in a `LogicalRelation` node that lives in the logical
plan tree.

**Stage 3 -- Physical Planning.** Two planning strategies convert `LogicalRelation` nodes into
physical plan nodes:

- `FileSourceStrategy` handles `HadoopFsRelation` and produces `FileSourceScanExec`.
- `DataSourceStrategy` handles non-file relations (implementing `TableScan`, `PrunedScan`,
  `PrunedFilteredScan`, or `CatalystScan`) and produces `RowDataSourceScanExec`.

**Stage 4 -- RDD Execution.** The physical nodes produce RDDs:

- `FileSourceScanExec` creates a `FileScanRDD` using a reader function obtained from
  `FileFormat.buildReaderWithPartitionValues`.
- `RowDataSourceScanExec` wraps the `RDD[Row]` returned by the scan trait's `buildScan` method,
  optionally converting `Row` objects to `InternalRow` via `DataSourceStrategy.toCatalystRDD`.

---

## 2. Key Interfaces

All V1 interfaces are defined in:
`sql/core/src/main/scala/org/apache/spark/sql/sources/interfaces.scala`

### DataSourceRegister

Allows a data source to register a short alias so users do not need to specify the fully
qualified class name.

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/sources/interfaces.scala

@Stable
trait DataSourceRegister {
  /**
   * The string that represents the format that this data source provider uses.
   * For example:
   *   override def shortName(): String = "parquet"
   */
  def shortName(): String
}
```

The `DataSource.lookupDataSource` method uses `ServiceLoader.load(classOf[DataSourceRegister])`
to discover all registered providers and matches them by `shortName()`. If no service-loader
match is found, it falls back to loading the class by name (trying both the supplied name and
`<name>.DefaultSource`).

### RelationProvider / SchemaRelationProvider

These two traits represent the entry point for creating a `BaseRelation`. A provider can
implement both to support optional schema specification.

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/sources/interfaces.scala

@Stable
trait RelationProvider {
  /** Returns a new base relation with the given parameters (keywords are case-insensitive). */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation
}

@Stable
trait SchemaRelationProvider {
  /** Returns a new base relation with the given parameters and user-defined schema. */
  def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation
}
```

The resolution logic in `DataSource.resolveRelation()` dispatches as follows:

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSource.scala

def resolveRelation(checkFilesExist: Boolean = true, readOnly: Boolean = false): BaseRelation = {
  val relation = (providingInstance(), userSpecifiedSchema) match {
    case (dataSource: SchemaRelationProvider, Some(schema)) =>
      dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions, schema)
    case (dataSource: RelationProvider, None) =>
      dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions)
    case (_: SchemaRelationProvider, None) =>
      throw QueryCompilationErrors.schemaNotSpecifiedForSchemaRelationProviderError(className)
    case (dataSource: RelationProvider, Some(schema)) =>
      // Creates relation and validates that its schema matches the user-specified one
      val baseRelation = dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions)
      if (!DataType.equalsIgnoreCompatibleNullability(baseRelation.schema, schema)) {
        throw QueryCompilationErrors.userSpecifiedSchemaMismatchActualSchemaError(...)
      }
      baseRelation
    case (format: FileFormat, _) =>
      // Constructs HadoopFsRelation (see below)
      ...
  }
  relation
}
```

### BaseRelation

The abstract base class for all V1 relations. Every concrete implementation must provide a
`schema` and typically mixes in one of the scan traits.

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/sources/interfaces.scala

@Stable
abstract class BaseRelation {
  def sqlContext: SQLContext

  /** The schema of data in this relation. */
  def schema: StructType

  /**
   * Estimated size in bytes. Used by the planner for broadcast join decisions.
   * Default: sessionState.conf.defaultSizeInBytes
   */
  def sizeInBytes: Long = sqlContext.sparkSession.sessionState.conf.defaultSizeInBytes

  /**
   * Whether the objects in RDD[Row] need conversion to internal representation.
   * If false, buildScan() must return RDD[InternalRow].
   * External data sources should leave this as true.
   */
  def needConversion: Boolean = true

  /**
   * Filters that this data source may not be able to handle. These are re-evaluated
   * by Spark after the scan. Default: returns all filters (safe double-evaluation).
   */
  def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters
}
```

**Key points for implementors:**

- Override `sizeInBytes` if you know the data size -- this controls broadcast join eligibility.
- Override `unhandledFilters` to tell the optimizer which pushed filters are fully handled so
  Spark can skip re-evaluating them.
- Override `needConversion` to `false` only if your `buildScan` returns `RDD[InternalRow]`
  directly (used by internal data sources like JDBC for performance).

### Scan Traits

These traits, mixed into a `BaseRelation` subclass, define how data is actually read. They form
a hierarchy of increasing optimization capability:

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/sources/interfaces.scala

/** Full table scan -- reads all columns, all rows. */
@Stable
trait TableScan {
  def buildScan(): RDD[Row]
}

/** Column pruning -- reads only requested columns. */
@Stable
trait PrunedScan {
  def buildScan(requiredColumns: Array[String]): RDD[Row]
}

/** Column pruning + filter pushdown. Filters are AND-conjuncted. */
@Stable
trait PrunedFilteredScan {
  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row]
}

/** Experimental: receives raw Catalyst expressions instead of Filter objects. */
@Unstable
trait CatalystScan {
  def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row]
}
```

**Which trait to implement:**

| Trait | Column Pruning | Filter Pushdown | Stability |
|---|---|---|---|
| `TableScan` | No | No | Stable |
| `PrunedScan` | Yes | No | Stable |
| `PrunedFilteredScan` | Yes | Yes (via `Filter` API) | Stable |
| `CatalystScan` | Yes | Yes (raw `Expression`) | Unstable / Experimental |

For most custom data sources, `PrunedFilteredScan` is the recommended trait. File-based sources
should instead implement `FileFormat` (see below).

The `DataSourceStrategy` planner matches these traits in priority order:

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSourceStrategy.scala

object DataSourceStrategy extends Strategy ... {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projects, filters, l @ LogicalRelationWithTable(t: CatalystScan, _)) =>
      // Uses raw Catalyst expressions
      pruneFilterProjectRaw(l, projects, filters,
        (requestedColumns, allPredicates, _) =>
          toCatalystRDD(l, requestedColumns, t.buildScan(requestedColumns, allPredicates))) :: Nil

    case PhysicalOperation(projects, filters,
        l @ LogicalRelationWithTable(t: PrunedFilteredScan, _)) =>
      // Uses Filter API with column pruning
      pruneFilterProject(l, projects, filters,
        (a, f) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray, f))) :: Nil

    case PhysicalOperation(projects, filters, l @ LogicalRelationWithTable(t: PrunedScan, _)) =>
      // Column pruning only
      pruneFilterProject(l, projects, filters,
        (a, _) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray))) :: Nil

    case l @ LogicalRelationWithTable(baseRelation: TableScan, _) =>
      // Full scan, no pruning
      RowDataSourceScanExec(l.output, ..., toCatalystRDD(l, baseRelation.buildScan()), ...) :: Nil

    case _ => Nil
  }
}
```

### FileFormat

The `FileFormat` trait is the V1 abstraction for file-based data sources. Instead of producing
an `RDD` directly, it provides a per-file reader function. The framework handles partitioning,
file splitting, and RDD construction via `FileScanRDD`.

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileFormat.scala

trait FileFormat {
  /** Infer schema from a set of files. Return None if inference is not supported. */
  def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType]

  /** Whether a file at the given path can be split across tasks. Default: false. */
  def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = false

  /** Whether this format supports columnar (vectorized) batch output. Default: false. */
  def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = false

  /** Column vector class names for columnar batch mode. Default: None. */
  def vectorTypes(
      requiredSchema: StructType,
      partitionSchema: StructType,
      sqlConf: SQLConf): Option[Seq[String]] = None

  /**
   * Returns a function that reads a single PartitionedFile as an Iterator[InternalRow].
   * Override this method (or buildReaderWithPartitionValues) to implement reading.
   */
  protected def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow]

  /**
   * Same as buildReader but appends partition column values to each row.
   * The default implementation calls buildReader and joins partition values.
   * Formats like Parquet override this directly for efficiency.
   */
  def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow]

  /** Whether this format supports the given data type in read/write paths. */
  def supportDataType(dataType: DataType): Boolean = true

  /** File metadata schema fields (file_path, file_name, file_size, etc.). */
  def metadataSchemaFields: Seq[StructField] = FileFormat.BASE_METADATA_FIELDS

  /** Write-side: prepares a write job and returns an OutputWriterFactory. */
  def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory
}
```

**Base metadata fields** available for every file format through `_metadata`:

| Field Name | Type | Description |
|---|---|---|
| `file_path` | `StringType` | Full URI of the file |
| `file_name` | `StringType` | File name only |
| `file_size` | `LongType` | Total file size in bytes |
| `file_block_start` | `LongType` | Start offset of the block being read |
| `file_block_length` | `LongType` | Length of the block being read |
| `file_modification_time` | `TimestampType` | Last modification time |

Parquet adds a generated `row_index` field (type `LongType`).

**Implementing a new FileFormat:** Override `inferSchema`, `buildReader` (or
`buildReaderWithPartitionValues`), and `prepareWrite`. Optionally override `isSplitable`,
`supportBatch`, and `vectorTypes` for performance.

### HadoopFsRelation

`HadoopFsRelation` is a `BaseRelation` that wraps a `FileFormat`. It is the concrete relation
type for all file-based V1 data sources. The `FileSourceStrategy` planner specifically matches
against this class.

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/HadoopFsRelation.scala

case class HadoopFsRelation(
    location: FileIndex,         // enumerates file paths
    partitionSchema: StructType, // schema of partition columns
    dataSchema: StructType,      // schema of data columns (from files)
    bucketSpec: Option[BucketSpec], // optional bucketing specification
    fileFormat: FileFormat,      // the file format implementation
    options: Map[String, String] // data source options
  )(val sparkSession: SparkSession)
  extends BaseRelation with FileRelation with SessionStateHelper {

  // Merged schema: data columns + partition columns (handling overlaps)
  val (schema: StructType, overlappedPartCols: Map[String, StructField]) =
    PartitioningUtils.mergeDataAndPartitionSchema(dataSchema, partitionSchema, ...)

  override def sizeInBytes: Long = {
    val compressionFactor = getSqlConf(sparkSession).fileCompressionFactor
    (location.sizeInBytes * compressionFactor).toLong
  }
}
```

`DataSource.resolveRelation()` constructs `HadoopFsRelation` for `FileFormat` providers:

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSource.scala

case (format: FileFormat, _) =>
  val (fileCatalog, dataSchema, partitionSchema) = ...
  HadoopFsRelation(
    fileCatalog,
    partitionSchema = partitionSchema,
    dataSchema = dataSchema.asNullable,
    bucketSpec = bucketSpec,
    format,
    caseInsensitiveOptions)(sparkSession)
```

The `FileIndex` (`location`) is responsible for listing files and exposing partition information.
Common implementations:

- `InMemoryFileIndex` -- scans file system paths, infers partitions from directory structure.
- `CatalogFileIndex` -- reads partition metadata from the Hive metastore catalog.

---

## 3. Schema Inference

### For FileFormat-based sources

Schema inference is handled by `DataSource.getOrInferFileFormatSchema()`:

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSource.scala

private def getOrInferFileFormatSchema(
    format: FileFormat,
    getFileIndex: () => InMemoryFileIndex): (StructType, StructType) = {

  // 1. Partition schema: infer from directory structure if not provided
  val partitionSchema = if (partitionColumns.isEmpty) {
    tempFileIndex.partitionSchema   // inferred from directory layout (e.g. /col=val/)
  } else {
    // Use user-specified partition columns, resolving types from userSpecifiedSchema
    // or falling back to inferred types
    ...
  }

  // 2. Data schema: use userSpecifiedSchema or call format.inferSchema()
  val dataSchema = userSpecifiedSchema.map { schema =>
    StructType(schema.filterNot(f => partitionSchema.exists(p => equality(p.name, f.name))))
  }.orElse {
    format.inferSchema(sparkSession, caseInsensitiveOptions - "path", tempFileIndex.allFiles())
  }.getOrElse {
    throw QueryCompilationErrors.dataSchemaNotSpecifiedError(format.toString)
  }

  (dataSchema, partitionSchema)
}
```

The three user-facing code paths:

1. **`spark.read.load("path")`** (no schema) -- Infers both data schema and partition columns
   from the file system.
2. **`spark.read.schema(userSchema).load("path")`** -- Parses partition columns from the
   directory layout, casts them to types from `userSchema` if present, and uses `userSchema`
   minus partition columns as the data schema.
3. **Catalog tables** -- Uses `CatalogFileIndex` with schema from the metastore; no inference
   needed.

### For non-file sources

`RelationProvider.createRelation` typically fetches schema from the external system. For example,
`JdbcRelationProvider` calls `JDBCRelation.getSchema(resolver, jdbcOptions)` which issues a
`SELECT * ... WHERE 1=0` query to get the table schema from the JDBC database.

---

## 4. Filter Pushdown and Partition Pruning

### Filter Translation

Catalyst `Expression` predicates are translated to the V1 `Filter` API by
`DataSourceStrategy.translateFilter`:

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSourceStrategy.scala

protected[sql] def translateFilter(
    predicate: Expression,
    supportNestedPredicatePushdown: Boolean): Option[Filter] = {
  predicate match {
    case expressions.And(left, right) =>
      // Both sides must be translatable (SPARK-12218)
      for {
        leftFilter <- translateFilter(left, ...)
        rightFilter <- translateFilter(right, ...)
      } yield sources.And(leftFilter, rightFilter)

    case expressions.Or(left, right) => ...
    case expressions.Not(child) => ...

    // Leaf node translations:
    case expressions.EqualTo(pushableColumn(name), Literal(v, t)) =>
      Some(sources.EqualTo(name, convertToScala(v, t)))
    case expressions.GreaterThan(...) => Some(sources.GreaterThan(...))
    case expressions.LessThan(...) => Some(sources.LessThan(...))
    case expressions.In(...) => Some(sources.In(...))
    case expressions.IsNull(pushableColumn(name)) => Some(sources.IsNull(name))
    case expressions.IsNotNull(pushableColumn(name)) => Some(sources.IsNotNull(name))
    case expressions.StartsWith(...) => Some(sources.StringStartsWith(...))
    case expressions.EndsWith(...) => Some(sources.StringEndsWith(...))
    case expressions.Contains(...) => Some(sources.StringContains(...))
    case _ => None  // not translatable
  }
}
```

Translatable predicates include: `EqualTo`, `EqualNullSafe`, `GreaterThan`,
`GreaterThanOrEqual`, `LessThan`, `LessThanOrEqual`, `In`, `InSet`, `IsNull`, `IsNotNull`,
`StringStartsWith`, `StringEndsWith`, `StringContains`, `And`, `Or`, `Not`, `AlwaysTrue`,
`AlwaysFalse`.

### selectFilters

After translation, filters are checked against `BaseRelation.unhandledFilters`:

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSourceStrategy.scala

protected[sql] def selectFilters(
    relation: BaseRelation,
    predicates: Seq[Expression]): (Seq[Expression], Seq[Filter], Set[Filter]) = {
  // translatedMap: Expression -> Filter (for translatable predicates)
  val pushedFilters: Seq[Filter] = translatedMap.values.toSeq
  val unhandledFilters = relation.unhandledFilters(translatedMap.values.toArray).toSet
  val handledFilters = pushedFilters.toSet -- unhandledFilters
  // Returns:
  //   1. Expressions that must be evaluated after the scan
  //   2. All filters pushed to the data source
  //   3. Filters fully handled by the data source (no post-scan evaluation needed)
  (nonconvertiblePredicates ++ unhandledPredicates, pushedFilters, handledFilters)
}
```

### FileSourceStrategy Filter Categorization

For `HadoopFsRelation`, `FileSourceStrategy` splits filters into four categories:

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileSourceStrategy.scala

// 1. Partition key filters -- used to prune directories
val partitionKeyFilters = DataSourceStrategy.getPushedDownFilters(partitionColumns, normalizedFilters)

// 2. Bucket key filters -- optionally used to prune bucket files
val bucketSet = if (shouldPruneBuckets(bucketSpec)) {
  genBucketSet(normalizedFilters, bucketSpec.get)
} else { None }

// 3. Data filters -- pushed down to the FileFormat reader
val dataFilters = normalizedFiltersWithScalarSubqueries.flatMap { f =>
  if (f.references.intersect(partitionSet).nonEmpty) {
    extractPredicatesWithinOutputSet(f, AttributeSet(dataColumnsWithoutPartitionCols))
  } else { Some(f) }
}
val pushedFilters = dataFilters.flatMap(DataSourceStrategy.translateFilter(_, ...))

// 4. Post-scan filters -- evaluated by FilterExec after the scan
val afterScanFilters = filterSet -- partitionKeyFilters.filter(_.references.nonEmpty)
```

### PruneFileSourcePartitions

Before physical planning, the `PruneFileSourcePartitions` optimizer rule prunes partitions
at the logical plan level for catalog-managed tables:

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/PruneFileSourcePartitions.scala

private[sql] object PruneFileSourcePartitions extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case op @ PhysicalOperation(projects, filters,
        logicalRelation @ LogicalRelationWithTable(
          fsRelation @ HadoopFsRelation(catalogFileIndex: CatalogFileIndex, ...),  _))
        if filters.nonEmpty && fsRelation.partitionSchema.nonEmpty =>

      val (partitionKeyFilters, _) = DataSourceUtils
        .getPartitionFiltersAndDataFilters(partitionSchema, normalizedFilters)

      if (partitionKeyFilters.nonEmpty) {
        val prunedFileIndex = catalogFileIndex.filterPartitions(partitionKeyFilters)
        val prunedFsRelation = fsRelation.copy(location = prunedFileIndex)(...)
        // Updates statistics and keeps partition filters in the plan
        ...
      }
  }
}
```

This rule applies specifically to `CatalogFileIndex` (Hive-managed tables) and replaces it with
a pruned file index that only lists matching partitions.

---

## 5. Execution Model

### PartitionedFile

The fundamental unit of work for file-based scans. Each `PartitionedFile` represents a byte
range within a single file, along with partition column values.

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileScanRDD.scala

case class PartitionedFile(
    partitionValues: InternalRow,   // values of partition columns
    filePath: SparkPath,            // URI of the file
    start: Long,                    // byte offset of block start
    length: Long,                   // number of bytes in block
    @transient locations: Array[String] = Array.empty,
    modificationTime: Long = 0L,
    fileSize: Long = 0L,
    otherConstantMetadataColumnValues: Map[String, Any] = Map.empty)
```

### FileScanRDD

The core RDD for reading file-based data sources. It takes a reader function and a list of
`FilePartition`s (each containing one or more `PartitionedFile`s).

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileScanRDD.scala

class FileScanRDD(
    @transient private val sparkSession: SparkSession,
    readFunction: (PartitionedFile) => Iterator[InternalRow],
    @transient val filePartitions: Seq[FilePartition],
    val readSchema: StructType,
    val metadataColumns: Seq[AttributeReference] = Seq.empty,
    metadataExtractors: Map[String, PartitionedFile => Any] = Map.empty,
    options: FileSourceOptions = ...)
  extends RDD[InternalRow](sparkSession.sparkContext, Nil)
```

The `compute` method iterates through files in each partition, calls `readFunction` for each
file, tracks input metrics, and appends metadata columns (like `_metadata.file_path`) when
requested.

Key behaviors:
- When `ignoreCorruptFiles` is enabled, corrupt files are silently skipped.
- When `ignoreMissingFiles` is enabled, missing files are silently skipped.
- Input metrics are updated periodically (every 1000 rows for row-based reads, every batch for
  columnar reads).

### FileSourceScanExec

The physical plan node for scanning `HadoopFsRelation`. This is where file listing, splitting,
and RDD construction happen.

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/DataSourceScanExec.scala

case class FileSourceScanExec(
    @transient override val relation: HadoopFsRelation,
    @transient stream: Option[SparkDataStream],
    override val output: Seq[Attribute],
    override val requiredSchema: StructType,
    override val partitionFilters: Seq[Expression],
    override val optionalBucketSet: Option[BitSet],
    override val optionalNumCoalescedBuckets: Option[Int],
    override val dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier],
    override val disableBucketedScan: Boolean = false)
  extends FileSourceScanLike
```

**Input RDD construction** (the heart of execution):

```scala
lazy val inputRDD: RDD[InternalRow] = {
  val options = relation.options +
    (FileFormat.OPTION_RETURNING_BATCH -> supportsColumnar.toString)

  // Get the per-file reader function from the FileFormat
  val readFile: (PartitionedFile) => Iterator[InternalRow] =
    relation.fileFormat.buildReaderWithPartitionValues(
      sparkSession = relation.sparkSession,
      dataSchema = relation.dataSchema,
      partitionSchema = relation.partitionSchema,
      requiredSchema = requiredSchema,
      filters = pushedDownFilters,
      options = options,
      hadoopConf = ...)

  // Create the RDD - bucketed or non-bucketed
  if (bucketedScan) {
    createBucketedReadRDD(relation.bucketSpec.get, readFile, dynamicallySelectedPartitions)
  } else {
    createReadRDD(readFile, dynamicallySelectedPartitions)
  }
}
```

**Non-bucketed file splitting algorithm** (in `createReadRDD`):

1. Compute `maxSplitBytes` based on configuration and total data size.
2. For each file, if it is splitable (per `FileFormat.isSplitable`), split it into chunks of
   `maxSplitBytes`.
3. Sort all file chunks by size in descending order.
4. Bin-pack chunks into `FilePartition`s using a greedy algorithm: add the next file chunk to
   the current partition if it stays under `maxSplitBytes`, otherwise start a new partition.

### RowDataSourceScanExec

The physical plan node for non-file V1 data sources. It wraps a pre-computed `RDD[InternalRow]`.

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/DataSourceScanExec.scala

case class RowDataSourceScanExec(
    output: Seq[Attribute],
    requiredSchema: StructType,
    filters: Set[Filter],
    handledFilters: Set[Filter],
    pushedDownOperators: PushedDownOperators,
    rdd: RDD[InternalRow],
    @transient relation: BaseRelation,
    @transient stream: Option[SparkDataStream],
    tableIdentifier: Option[TableIdentifier])
  extends DataSourceScanExec with InputRDDCodegen
```

The `doExecute` method applies an `UnsafeProjection` to convert rows to `UnsafeRow` format:

```scala
protected override def doExecute(): RDD[InternalRow] = {
  val numOutputRows = longMetric("numOutputRows")
  rdd.mapPartitionsWithIndexInternal { (index, iter) =>
    val proj = UnsafeProjection.create(schema)
    proj.initialize(index)
    iter.map { r =>
      numOutputRows += 1
      proj(r)
    }
  }
}
```

### Row Conversion

When `BaseRelation.needConversion` is `true` (the default for external sources),
`DataSourceStrategy.toCatalystRDD` converts `RDD[Row]` to `RDD[InternalRow]`:

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSourceStrategy.scala

private[sql] def toCatalystRDD(
    relation: BaseRelation,
    output: Seq[Attribute],
    rdd: RDD[Row]): RDD[InternalRow] = {
  if (relation.needConversion) {
    val toRow = ExpressionEncoder(DataTypeUtils.fromAttributes(output), lenient = true)
      .createSerializer()
    rdd.mapPartitions { iterator =>
      iterator.map(toRow)
    }
  } else {
    rdd.asInstanceOf[RDD[InternalRow]]
  }
}
```

---

## 6. DataFrameReader Flow

The user-facing entry point for batch reads is `DataFrameReader`. Here is the complete flow
from `load()` to query execution:

### Step 1: DataFrameReader.load()

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/classic/DataFrameReader.scala

def load(paths: String*): DataFrame = {
  Dataset.ofRows(
    sparkSession,
    UnresolvedDataSource(source, userSpecifiedSchema, extraOptions, isStreaming = false, paths)
  )
}
```

This creates an `UnresolvedDataSource` logical plan node and triggers analysis via
`Dataset.ofRows`.

### Step 2: ResolveDataSource (Analyzer Rule)

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/catalyst/analysis/ResolveDataSource.scala

class ResolveDataSource(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case UnresolvedDataSource(source, userSpecifiedSchema, extraOptions, false, paths) =>
      // First try V2
      DataSource.lookupDataSourceV2(source, conf).flatMap { provider =>
        DataSourceV2Utils.loadV2Source(sparkSession, provider, ...)
      }.getOrElse(
        // Fall back to V1
        loadV1BatchSource(source, userSpecifiedSchema, extraOptions, paths: _*)
      )
  }

  private def loadV1BatchSource(...): LogicalPlan = {
    LogicalRelation(
      DataSource.apply(sparkSession, paths = finalPaths, userSpecifiedSchema = userSpecifiedSchema,
        className = source, options = finalOptions.originalMap
      ).resolveRelation(readOnly = true))
  }
}
```

The V2 path is attempted first. If the provider is listed in `spark.sql.sources.useV1SourceList`
(which includes `csv`, `json`, `parquet`, `orc`, `text` by default in some configurations), or
if no V2 provider exists, the V1 path is used.

### Step 3: DataSource.resolveRelation()

As described in [Section 2](#relationprovider--schemarelationprovider), this dispatches to
the appropriate provider and returns a `BaseRelation` (either from `RelationProvider` /
`SchemaRelationProvider` or `HadoopFsRelation` for file formats).

### Step 4: LogicalRelation

The `BaseRelation` is wrapped in `LogicalRelation`:

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/LogicalRelation.scala

case class LogicalRelation(
    relation: BaseRelation,
    output: Seq[AttributeReference],
    catalogTable: Option[CatalogTable],
    override val isStreaming: Boolean,
    @transient stream: Option[SparkDataStream])
  extends LeafNode with MultiInstanceRelation with ExposesMetadataColumns
```

For `HadoopFsRelation`, the `metadataOutput` provides the `_metadata` struct column:

```scala
override lazy val metadataOutput: Seq[AttributeReference] = relation match {
  case relation: HadoopFsRelation =>
    metadataOutputWithOutConflicts(Seq(relation.fileFormat.createFileMetadataCol()))
  case _ => Nil
}
```

### Step 5: Physical Planning

During physical planning, the `SparkPlanner` applies strategies in order. For V1 relations:

- `FileSourceStrategy` matches `LogicalRelation(HadoopFsRelation, ...)` and produces
  `FileSourceScanExec`.
- `DataSourceStrategy` matches `LogicalRelation` with `CatalystScan`, `PrunedFilteredScan`,
  `PrunedScan`, or `TableScan` and produces `RowDataSourceScanExec`.

### Step 6: Execution

The physical plan nodes execute their `doExecute()` method which produces the final
`RDD[InternalRow]` as described in [Section 5](#5-execution-model).

---

## 7. Format Examples

### JSON

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/json/JsonFileFormat.scala

case class JsonFileFormat() extends TextBasedFileFormat with DataSourceRegister {
  override val shortName: String = "json"

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val parsedOptions = getJsonOptions(sparkSession, options)
    JsonDataSource(parsedOptions).inferSchema(sparkSession, files, parsedOptions)
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    val parsedOptions = getJsonOptions(sparkSession, options)
    JsonDataSource(parsedOptions).isSplitable && super.isSplitable(sparkSession, options, path)
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val broadcastedHadoopConf =
      SerializableConfiguration.broadcast(sparkSession.sparkContext, hadoopConf)
    val parsedOptions = getJsonOptions(sparkSession, options)
    ...
    (file: PartitionedFile) => {
      val parser = new JacksonParser(actualSchema, parsedOptions, allowArrayAsStructs = true, filters)
      JsonDataSource(parsedOptions).readFile(broadcastedHadoopConf.value.value, file, parser, requiredSchema)
    }
  }
}
```

Key characteristics:
- Extends `TextBasedFileFormat`, which makes it splitable when not compressed (or when using a
  `SplittableCompressionCodec`).
- Schema inference reads file samples and infers types from JSON structure.
- `buildReader` creates a `JacksonParser` per partition (not per file) for efficiency.
- Supports filter pushdown via `filters` parameter passed to the parser.

### CSV

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/csv/CSVFileFormat.scala

case class CSVFileFormat() extends TextBasedFileFormat with DataSourceRegister {
  override def shortName(): String = "csv"

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val parsedOptions = getCsvOptions(sparkSession, options)
    CSVDataSource(parsedOptions).inferSchema(sparkSession, files, parsedOptions)
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    ...
    (file: PartitionedFile) => {
      val parser = new UnivocityParser(actualDataSchema, actualRequiredSchema, parsedOptions, actualFilters)
      val headerChecker = new CSVHeaderChecker(schema, parsedOptions, source = ..., isStartOfFile)
      CSVDataSource(parsedOptions).readFile(conf, file, parser, headerChecker, requiredSchema)
    }
  }

  // CSV allows duplicate column names
  override def allowDuplicatedColumnNames: Boolean = true
}
```

Key characteristics:
- Also extends `TextBasedFileFormat` (splitable when uncompressed).
- Schema inference uses the first row as header (if configured) and samples data for type
  inference.
- Supports column pruning at the parser level (`CSVOptions.isColumnPruningEnabled`).
- Filters are pushed down to `UnivocityParser` but only for non-corrupt-record columns.

### Parquet

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFileFormat.scala

class ParquetFileFormat extends FileFormat with DataSourceRegister ... {
  override def shortName(): String = "parquet"

  override def inferSchema(sparkSession: SparkSession,
      parameters: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {
    ParquetUtils.inferSchema(sparkSession, parameters, files)
  }

  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    ParquetUtils.isBatchReadSupportedForSchema(getSqlConf(sparkSession), schema)
  }

  override def vectorTypes(
      requiredSchema: StructType, partitionSchema: StructType, sqlConf: SQLConf): Option[Seq[String]] = {
    Option(Seq.fill(requiredSchema.fields.length)(
      if (!sqlConf.offHeapColumnVectorEnabled) classOf[OnHeapColumnVector].getName
      else classOf[OffHeapColumnVector].getName
    ) ++ Seq.fill(partitionSchema.fields.length)(classOf[ConstantColumnVector].getName))
  }

  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = true

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    ...
    (file: PartitionedFile) => {
      // Read footer, push filters at RowGroup level
      val pushed: Option[FilterPredicate] = if (enableParquetFilterPushDown) {
        filters.flatMap(parquetFilters.createFilter).reduceOption(FilterApi.and)
      } else { None }

      if (enableVectorizedReader) {
        // Use VectorizedParquetRecordReader for columnar batch output
        buildVectorizedIterator(...)
      } else {
        // Fall back to parquet-mr row-based reader
        buildRowBasedIterator(...)
      }
    }
  }

  // Parquet adds row_index as a generated metadata column
  override def metadataSchemaFields: Seq[StructField] = {
    super.metadataSchemaFields :+ ParquetFileFormat.ROW_INDEX_FIELD
  }
}
```

Key characteristics:
- Always splitable (Parquet is a columnar format with independent row groups).
- Supports vectorized (columnar batch) reading via `VectorizedParquetRecordReader` when the
  schema contains only supported types.
- Filter pushdown at the Parquet RowGroup level using `parquet-mr` predicate API.
- Overrides `buildReaderWithPartitionValues` directly (instead of `buildReader`) for
  efficiency -- it appends partition values during vectorized reading.
- Provides `_metadata.row_index` generated metadata column.

### JDBC

JDBC is a non-file data source that implements `RelationProvider` and `PrunedFilteredScan`.

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcRelationProvider.scala

class JdbcRelationProvider extends CreatableRelationProvider
  with RelationProvider with DataSourceRegister {

  override def shortName(): String = "jdbc"

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val jdbcOptions = new JDBCOptions(parameters)
    val schema = JDBCRelation.getSchema(resolver, jdbcOptions)
    val parts = JDBCRelation.columnPartition(schema, resolver, timeZoneId, jdbcOptions)
    JDBCRelation(schema, parts, jdbcOptions, ...)(sparkSession)
  }
}
```

```scala
// File: sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRelation.scala

private[sql] case class JDBCRelation(
    override val schema: StructType,
    parts: Array[Partition],
    jdbcOptions: JDBCOptions,
    additionalMetrics: Map[String, SQLMetric] = Map()
  )(@transient val sparkSession: SparkSession)
  extends BaseRelation with PrunedFilteredScan with InsertableRelation {

  // Bypass Row -> InternalRow conversion for performance
  override val needConversion: Boolean = false

  // Let the JDBC dialect determine which filters it can handle
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    if (jdbcOptions.pushDownPredicate) {
      val dialect = JdbcDialects.get(jdbcOptions.url)
      filters.filter(f => dialect.compileExpression(f.toV2).isEmpty)
    } else {
      filters
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val pushedPredicates = if (jdbcOptions.pushDownPredicate) {
      filters.map(_.toV2)
    } else {
      Array.empty[Predicate]
    }
    JDBCRDD.scanTable(sparkSession.sparkContext, schema, requiredColumns,
      pushedPredicates, parts, jdbcOptions, ...).asInstanceOf[RDD[Row]]
  }
}
```

Key characteristics:
- Schema is fetched from the remote database at relation creation time.
- Partitioning is driven by a user-specified column and bounds (`partitionColumn`,
  `lowerBound`, `upperBound`, `numPartitions`), generating WHERE clauses for each partition.
- Sets `needConversion = false` because `JDBCRDD` returns `InternalRow` directly (cast as
  `RDD[Row]` via type erasure).
- Filter pushdown is dialect-aware: only filters that the JDBC dialect can compile to SQL are
  pushed down.

---

## Summary: Implementing a New V1 Data Source

### Non-file data source

1. Create a class that extends `RelationProvider` (or `SchemaRelationProvider`) and
   `DataSourceRegister`.
2. Implement `shortName()` for alias registration.
3. Implement `createRelation()` to return a `BaseRelation` subclass.
4. In your `BaseRelation`, implement `schema` and mix in `PrunedFilteredScan` (recommended).
5. Implement `buildScan(requiredColumns, filters)` to return an `RDD[Row]`.
6. Optionally override `sizeInBytes` and `unhandledFilters` for better optimization.
7. Register your class via `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`.

### File-based data source

1. Create a class that extends `FileFormat` and `DataSourceRegister`.
2. Implement `shortName()`.
3. Implement `inferSchema()` to return `Option[StructType]`.
4. Implement `buildReader()` to return a `PartitionedFile => Iterator[InternalRow]` function.
5. Implement `prepareWrite()` for write support.
6. Optionally override `isSplitable()`, `supportBatch()`, `vectorTypes()`, and
   `metadataSchemaFields()`.
7. Register via `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`.

The framework will handle file discovery, partition inference, file splitting, RDD construction,
and query planning automatically through `HadoopFsRelation`, `FileSourceStrategy`, and
`FileScanRDD`.
