/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.common.model.HoodieBaseFile
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.hadoop.utils.HoodieRealtimeInputFormatUtils
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileStatusCache, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

case class HoodieMergeOnReadFileSplit(dataFile: Option[PartitionedFile],
                                      logPaths: Option[List[String]],
                                      latestCommit: String,
                                      tablePath: String,
                                      maxCompactionMemoryInBytes: Long,
                                      mergeType: String)

case class HoodieMergeOnReadTableState(tableStructSchema: StructType,
                                       requiredStructSchema: StructType,
                                       tableAvroSchema: String,
                                       requiredAvroSchema: String,
                                       hoodieRealtimeFileSplits: List[HoodieMergeOnReadFileSplit],
                                       preCombineField: Option[String])

class MergeOnReadSnapshotRelation(val sqlContext: SQLContext,
                                  val optParams: Map[String, String],
                                  val userSchema: StructType,
                                  val globPaths: Option[Seq[Path]],
                                  val metaClient: HoodieTableMetaClient)
  extends BaseRelation with PrunedFilteredScan with Logging {

  private val conf = sqlContext.sparkContext.hadoopConfiguration
  private val jobConf = new JobConf(conf)
  // use schema from latest metadata, if not present, read schema from the data file
  private val schemaUtil = new TableSchemaResolver(metaClient)
  private val tableAvroSchema = schemaUtil.getTableAvroSchema
  private val tableStructSchema = AvroConversionUtils.convertAvroSchemaToStructType(tableAvroSchema)
  private val mergeType = optParams.getOrElse(
    DataSourceReadOptions.REALTIME_MERGE_OPT_KEY,
    DataSourceReadOptions.DEFAULT_REALTIME_MERGE_OPT_VAL)
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

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    log.debug(s" buildScan requiredColumns = ${requiredColumns.mkString(",")}")
    log.debug(s" buildScan filters = ${filters.mkString(",")}")
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
      filters = filters,
      options = optParams,
      hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
    )
    val requiredSchemaParquetReader = new ParquetFileFormat().buildReaderWithPartitionValues(
      sparkSession = sqlContext.sparkSession,
      dataSchema = tableStructSchema,
      partitionSchema = StructType(Nil),
      requiredSchema = requiredStructSchema,
      filters = filters,
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
    val fileStatuses = if (globPaths.isDefined) {
      // Load files from the global paths if it has defined to be compatible with the original mode
      val inMemoryFileIndex = HoodieSparkUtils.createInMemoryFileIndex(sqlContext.sparkSession, globPaths.get)
      inMemoryFileIndex.allFiles()
    } else { // Load files by the HoodieFileIndex.
      val hoodieFileIndex = HoodieFileIndex(sqlContext.sparkSession, metaClient,
        Some(tableStructSchema), optParams, FileStatusCache.getOrCreate(sqlContext.sparkSession))
      hoodieFileIndex.allFiles
    }
    if (fileStatuses.isEmpty) { // Return empty list if the table has not inited.
      List.empty
    } else {
      val fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getActiveTimeline.getCommitsTimeline
          .filterCompletedInstants, fileStatuses.toArray)
      val latestFiles: List[HoodieBaseFile] = fsView.getLatestBaseFiles.iterator().asScala.toList
      if (!fsView.getLastInstant.isPresent) { // Return empty list if the table has no commit
        List.empty
      } else {
        val latestCommit = fsView.getLastInstant.get().getTimestamp
        val fileGroup = HoodieRealtimeInputFormatUtils.groupLogsByBaseFile(conf, latestFiles.asJava).asScala
        val fileSplits = fileGroup.map(kv => {
          val baseFile = kv._1
          val logPaths = if (kv._2.isEmpty) Option.empty else Option(kv._2.asScala.toList)
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
          val filePath = baseFile.getFileStatus.getPath.toUri.toString
          val partitionedFile = PartitionedFile(InternalRow.empty, filePath, 0, baseFile.getFileLen)
          HoodieMergeOnReadFileSplit(Option(partitionedFile), logPaths, latestCommit,
            metaClient.getBasePath, maxCompactionMemoryInBytes, mergeType)
        }).toList
        fileSplits
      }
    }
  }
}
