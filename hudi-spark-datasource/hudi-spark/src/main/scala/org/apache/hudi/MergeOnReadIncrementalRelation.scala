/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.listAffectedFilesForCommits
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes
import org.apache.hadoop.fs.{FileStatus, GlobPattern, Path}
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * Experimental.
  * Relation, that implements the Hoodie incremental view for Merge On Read table.
  *
  */
class MergeOnReadIncrementalRelation(val sqlContext: SQLContext,
                                     val optParams: Map[String, String],
                                     val userSchema: StructType,
                                     val metaClient: HoodieTableMetaClient)
  extends BaseRelation with PrunedFilteredScan {

  private val log = LogManager.getLogger(classOf[MergeOnReadIncrementalRelation])
  private val conf = sqlContext.sparkContext.hadoopConfiguration
  private val jobConf = new JobConf(conf)
  private val fs = FSUtils.getFs(metaClient.getBasePath, conf)
  private val commitTimeline = metaClient.getCommitsAndCompactionTimeline.filterCompletedInstants()
  if (commitTimeline.empty()) {
    throw new HoodieException("No instants to incrementally pull")
  }
  if (!optParams.contains(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY)) {
    throw new HoodieException(s"Specify the begin instant time to pull from using " +
      s"option ${DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY}")
  }

  private val lastInstant = commitTimeline.lastInstant().get()
  private val mergeType = optParams.getOrElse(
    DataSourceReadOptions.REALTIME_MERGE_OPT_KEY,
    DataSourceReadOptions.DEFAULT_REALTIME_MERGE_OPT_VAL)

  private val commitsTimelineToReturn = commitTimeline.findInstantsInRange(
    optParams(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY),
    optParams.getOrElse(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, lastInstant.getTimestamp))
  log.debug(s"${commitsTimelineToReturn.getInstants.iterator().toList.map(f => f.toString).mkString(",")}")
  private val commitsToReturn = commitsTimelineToReturn.getInstants.iterator().toList
  private val schemaUtil = new TableSchemaResolver(metaClient)
  private val tableAvroSchema = schemaUtil.getTableAvroSchema
  private val tableStructSchema = AvroConversionUtils.convertAvroSchemaToStructType(tableAvroSchema)
  private val maxCompactionMemoryInBytes = getMaxCompactionMemoryInBytes(jobConf)
  private val fileIndex = buildFileIndex()
  private val preCombineField = {
    val preCombineFieldFromTableConfig = metaClient.getTableConfig.getPreCombineField
    if (preCombineFieldFromTableConfig != null) {
      Some(preCombineFieldFromTableConfig)
    } else {
      // get preCombineFiled from the options if this is a old table which have not store
      // the field to hoodie.properties
      optParams.get(DataSourceReadOptions.READ_PRE_COMBINE_FIELD)
    }
  }
  override def schema: StructType = tableStructSchema

  override def needConversion: Boolean = false

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    val isNotNullFilter = IsNotNull(HoodieRecord.COMMIT_TIME_METADATA_FIELD)
    val largerThanFilter = GreaterThanOrEqual(HoodieRecord.COMMIT_TIME_METADATA_FIELD, commitsToReturn.head.getTimestamp)
    val lessThanFilter = LessThanOrEqual(HoodieRecord.COMMIT_TIME_METADATA_FIELD, commitsToReturn.last.getTimestamp)
    filters :+isNotNullFilter :+ largerThanFilter :+ lessThanFilter
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    log.debug(s"buildScan requiredColumns = ${requiredColumns.mkString(",")}")
    log.debug(s"buildScan filters = ${filters.mkString(",")}")
    // config to ensure the push down filter for parquet will be applied.
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.filterPushdown", "true")
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.recordLevelFilter.enabled", "true")
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.enableVectorizedReader", "false")
    val pushDownFilter = {
      val isNotNullFilter = IsNotNull(HoodieRecord.COMMIT_TIME_METADATA_FIELD)
      val largerThanFilter = GreaterThanOrEqual(HoodieRecord.COMMIT_TIME_METADATA_FIELD, commitsToReturn.head.getTimestamp)
      val lessThanFilter = LessThanOrEqual(HoodieRecord.COMMIT_TIME_METADATA_FIELD, commitsToReturn.last.getTimestamp)
      filters :+isNotNullFilter :+ largerThanFilter :+ lessThanFilter
    }
    var requiredStructSchema = StructType(Seq())
    requiredColumns.foreach(col => {
      val field = tableStructSchema.find(_.name == col)
      if (field.isDefined) {
        requiredStructSchema = requiredStructSchema.add(field.get)
      }
    })
    val requiredAvroSchema = AvroConversionUtils
      .convertStructTypeToAvroSchema(requiredStructSchema, tableAvroSchema.getName, tableAvroSchema.getNamespace)
    val hoodieTableState = HoodieMergeOnReadTableState(
      tableStructSchema,
      requiredStructSchema,
      tableAvroSchema.toString,
      requiredAvroSchema.toString,
      fileIndex,
      preCombineField
    )
    val fullSchemaParquetReader = new ParquetFileFormat().buildReaderWithPartitionValues(
      sparkSession = sqlContext.sparkSession,
      dataSchema = tableStructSchema,
      partitionSchema = StructType(Nil),
      requiredSchema = tableStructSchema,
      filters = pushDownFilter,
      options = optParams,
      hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
    )
    val requiredSchemaParquetReader = new ParquetFileFormat().buildReaderWithPartitionValues(
      sparkSession = sqlContext.sparkSession,
      dataSchema = tableStructSchema,
      partitionSchema = StructType(Nil),
      requiredSchema = requiredStructSchema,
      filters = pushDownFilter,
      options = optParams,
      hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
    )

    val rdd = new HoodieMergeOnReadRDD(
      sqlContext.sparkContext,
      jobConf,
      fullSchemaParquetReader,
      requiredSchemaParquetReader,
      hoodieTableState
    )
    rdd.asInstanceOf[RDD[Row]]
  }

  def buildFileIndex(): List[HoodieMergeOnReadFileSplit] = {
    val partitionsWithFileStatus = listAffectedFilesForCommits(new Path(metaClient.getBasePath),
      commitsToReturn, commitsTimelineToReturn)
    val affectedFileStatus = new ListBuffer[FileStatus]
    partitionsWithFileStatus.iterator.foreach(p =>
      p._2.iterator.foreach(status => affectedFileStatus += status._2))
    val fsView = new HoodieTableFileSystemView(metaClient,
      commitsTimelineToReturn, affectedFileStatus.toArray)

    // Iterate partitions to create splits
    val fileGroup = partitionsWithFileStatus.keySet().flatMap(partitionPath =>
      fsView.getAllFileGroups(partitionPath).iterator()
    ).toList
    val latestCommit = fsView.getLastInstant.get().getTimestamp
    if (log.isDebugEnabled) {
      fileGroup.foreach(f => log.debug(s"current file group id: " +
        s"${f.getFileGroupId} and file slices ${f.getLatestFileSlice.get().toString}"))
    }

    // Filter files based on user defined glob pattern
    val pathGlobPattern = optParams.getOrElse(
      DataSourceReadOptions.INCR_PATH_GLOB_OPT_KEY,
      DataSourceReadOptions.DEFAULT_INCR_PATH_GLOB_OPT_VAL)
    val filteredFileGroup = if(!pathGlobPattern
      .equals(DataSourceReadOptions.DEFAULT_INCR_PATH_GLOB_OPT_VAL)) {
      val globMatcher = new GlobPattern("*" + pathGlobPattern)
      fileGroup.filter(f => {
        if (f.getLatestFileSlice.get().getBaseFile.isPresent) {
          globMatcher.matches(f.getLatestFileSlice.get().getBaseFile.get.getPath)
        } else {
          globMatcher.matches(f.getLatestFileSlice.get().getLatestLogFile.get().getPath.toString)
        }
      })
    } else {
      fileGroup
    }

    // Build HoodieMergeOnReadFileSplit.
    filteredFileGroup.map(f => {
      // Ensure get the base file when there is a pending compaction, which means the base file
      // won't be in the latest file slice.
      val baseFiles = f.getAllFileSlices.iterator().filter(slice => slice.getBaseFile.isPresent).toList
      val partitionedFile = if (baseFiles.nonEmpty) {
        val baseFile = baseFiles.head.getBaseFile
        // Here we use the Path#toUri to encode the path string, as there is a decode in
        // ParquetFileFormat#buildReaderWithPartitionValues in the spark project when read the table
        // .So we should encode the file path here. Otherwise, there is a FileNotException throw
        // out.
        // For example, If the "pt" is the partition path field, and "pt" = "2021/02/02", If
        // we enable the URL_ENCODE_PARTITIONING_OPT_KEY and write data to hudi table.The data
        // path in the table will just like "/basePath/2021%2F02%2F02/xxxx.parquet". When we read
        // data from the table, if there are no encode for the file path,
        // ParquetFileFormat#buildReaderWithPartitionValues will decode it to
        // "/basePath/2021/02/02/xxxx.parquet" witch will result to a FileNotException.
        // See FileSourceScanExec#createBucketedReadRDD in spark project which do the same thing
        // when create PartitionedFile.
        val filePath = baseFile.get.getFileStatus.getPath.toUri.toString
        Option(PartitionedFile(InternalRow.empty, filePath, 0, baseFile.get.getFileLen))
      }
      else {
        Option.empty
      }

      val logPath = if (f.getLatestFileSlice.isPresent) {
        //If log path doesn't exist, we still include an empty path to avoid using
        // the default parquet reader to ensure the push down filter will be applied.
        Option(f.getLatestFileSlice.get().getLogFiles.iterator().toList
          .map(logfile => logfile.getPath.toString))
      }
      else {
        Option.empty
      }

      HoodieMergeOnReadFileSplit(partitionedFile, logPath,
        latestCommit, metaClient.getBasePath, maxCompactionMemoryInBytes, mergeType)
    })
  }
}
