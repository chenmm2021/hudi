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

import java.net.URLEncoder

import org.apache.hudi.DataSourceWriteOptions.{KEYGENERATOR_CLASS_OPT_KEY, PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, GreaterThanOrEqual, LessThan, Literal}
import org.apache.spark.sql.execution.datasources.PartitionDirectory
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class TestHoodieFileIndex extends HoodieClientTestBase {

  var spark: SparkSession = _
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "timestamp",
    HoodieWriteConfig.TABLE_NAME -> "hoodie_test"
  )

  @BeforeEach override def setUp() {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
    initMetaClient()
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPartitionSchema(partitionEncode: Boolean): Unit = {
    val records1 = dataGen.generateInsertsContainsAllPartitions("000", 100)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1), 2))
    inputDF1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING_OPT_KEY, partitionEncode)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val fileIndex = HoodieFileIndex(spark, metaClient, None, Map("path" -> basePath))
    assertEquals("partition", fileIndex.partitionSchema.fields.map(_.name).mkString(","))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPartitionPruneWithPartitionEncode(partitionEncode: Boolean): Unit = {
    val partitions = Array("2021/03/08", "2021/03/09", "2021/03/10", "2021/03/11", "2021/03/12")
    val newDataGen =  new HoodieTestDataGenerator(partitions)
    val records1 = newDataGen.generateInsertsContainsAllPartitions("000", 100)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1), 2))
    inputDF1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING_OPT_KEY, partitionEncode)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val fileIndex = HoodieFileIndex(spark, metaClient, None, Map("path" -> basePath))

    val partitionFilter1 = EqualTo(attribute("partition"), literal("2021/03/08"))
    val partitionName = if (partitionEncode) URLEncoder.encode("2021/03/08") else "2021/03/08"
    val partitionAndFilesAfterPrune = fileIndex.listFiles(Seq(partitionFilter1), Seq.empty)
    assertEquals(1, partitionAndFilesAfterPrune.size)

    val PartitionDirectory(partitionValues, filesInPartition) = partitionAndFilesAfterPrune(0)
    assertEquals(partitionValues.toSeq(Seq(StringType)).mkString(","), "2021/03/08")
    assertEquals(getFileCountInPartitionPath(partitionName), filesInPartition.size)

    val partitionFilter2 = And(
      GreaterThanOrEqual(attribute("partition"), literal("2021/03/08")),
      LessThan(attribute("partition"), literal("2021/03/10"))
    )
    val prunedPartitions = fileIndex.listFiles(Seq(partitionFilter2),
      Seq.empty).map(_.values.toSeq(Seq(StringType)).mkString(",")).toList

    assertEquals(List("2021/03/08", "2021/03/09"), prunedPartitions)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPartitionPruneWithMultiPartitionColumns(useMetaFileList: Boolean): Unit = {
    val _spark = spark
    import _spark.implicits._
    // Test the case the partition column size is equal to the partition directory level.
    val inputDF1 = (for (i <- 0 until 10) yield (i, s"a$i", 10 + i, 10000,
      s"2021-03-0${i % 2 + 1}", "10")).toDF("id", "name", "price", "version", "dt", "hh")

    inputDF1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(RECORDKEY_FIELD_OPT_KEY, "id")
      .option(PRECOMBINE_FIELD_OPT_KEY, "version")
      .option(PARTITIONPATH_FIELD_OPT_KEY, "dt,hh")
      .option(KEYGENERATOR_CLASS_OPT_KEY, classOf[ComplexKeyGenerator].getName)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING_OPT_KEY, "false")
      .option(HoodieMetadataConfig.METADATA_ENABLE_PROP, useMetaFileList)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val fileIndex = HoodieFileIndex(spark, metaClient, None, Map("path" -> basePath))

    val partitionFilter1 = And(
      EqualTo(attribute("dt"), literal("2021-03-01")),
      EqualTo(attribute("hh"), literal("10"))
    )
    val partitionAndFilesAfterPrune = fileIndex.listFiles(Seq(partitionFilter1), Seq.empty)
    assertEquals(1, partitionAndFilesAfterPrune.size)

    val PartitionDirectory(partitionValues, filesAfterPrune) = partitionAndFilesAfterPrune(0)
    // The partition prune will work for this case.
    assertEquals(partitionValues.toSeq(Seq(StringType)).mkString(","), "2021-03-01,10")
    assertEquals(getFileCountInPartitionPath("2021-03-01/10"), filesAfterPrune.size)

    val readDF1 = spark.read.format("hudi").load(basePath)
    assertEquals(10, readDF1.count())
    assertEquals(5, readDF1.filter("dt = '2021-03-01' and hh = '10'").count())

    // Test the case that partition column size not match the partition directory level and
    // partition column size is > 1. We will not trait it as partitioned table when read.
    val inputDF2 = (for (i <- 0 until 10) yield (i, s"a$i", 10 + i, 100 * i + 10000,
      s"2021/03/0${i % 2 + 1}", "10")).toDF("id", "name", "price", "version", "dt", "hh")
    inputDF2.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(RECORDKEY_FIELD_OPT_KEY, "id")
      .option(PRECOMBINE_FIELD_OPT_KEY, "version")
      .option(PARTITIONPATH_FIELD_OPT_KEY, "dt,hh")
      .option(KEYGENERATOR_CLASS_OPT_KEY, classOf[ComplexKeyGenerator].getName)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING_OPT_KEY, "false")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    fileIndex.refresh()
    val partitionFilter2 = And(
      EqualTo(attribute("dt"), literal("2021/03/01")),
      EqualTo(attribute("hh"), literal("10"))
    )
    val partitionAndFilesAfterPrune2 = fileIndex.listFiles(Seq(partitionFilter2), Seq.empty)

    assertEquals(1, partitionAndFilesAfterPrune2.size)
    val PartitionDirectory(partitionValues2, filesAfterPrune2) = partitionAndFilesAfterPrune2(0)
    // The partition prune would not work for this case, so the partition value it
    // returns is a InternalRow.empty.
    assertEquals(partitionValues2, InternalRow.empty)
    // The returned file size should equal to the whole file size in all the partition paths.
    assertEquals(getFileCountInPartitionPaths("2021/03/01/10", "2021/03/02/10"),
      filesAfterPrune2.length)
    val readDF2 = spark.read.format("hudi").load(basePath)

    assertEquals(10, readDF2.count())
    // There are 5 rows in the  dt = 2021/03/01 and hh = 10
    assertEquals(5, readDF2.filter("dt = '2021/03/01' and hh ='10'").count())
  }

  private def attribute(partition: String): AttributeReference = {
    AttributeReference(partition, StringType, true)()
  }

  private def literal(value: String): Literal = {
    Literal.create(value)
  }

  private def getFileCountInPartitionPath(partitionPath: String): Int = {
    metaClient.reloadActiveTimeline()
    val activeInstants = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants
    val fileSystemView = new HoodieTableFileSystemView(metaClient, activeInstants)
    fileSystemView.getAllBaseFiles(partitionPath).iterator().asScala.toSeq.length
  }

  private def getFileCountInPartitionPaths(partitionPaths: String*): Int = {
    partitionPaths.map(getFileCountInPartitionPath).sum
  }
}
