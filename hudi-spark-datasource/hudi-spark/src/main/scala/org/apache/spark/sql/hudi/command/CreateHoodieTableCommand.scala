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

package org.apache.spark.sql.hudi.command

import scala.collection.JavaConverters._
import java.util.{Locale, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.{DataSourceWriteOptions, SparkSqlAdapterSupport}
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.hadoop.HoodieParquetInputFormat
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils
import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hive.HiveClientUtils
import org.apache.spark.sql.hive.HiveExternalCatalog._
import org.apache.spark.sql.hudi.HoodieSqlUtils._
import org.apache.spark.sql.hudi.HoodieOptionConfig
import org.apache.spark.sql.hudi.command.CreateHoodieTableCommand.{initTableIfNeed, tableExists}
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.internal.StaticSQLConf.SCHEMA_STRING_LENGTH_THRESHOLD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

import scala.collection.mutable

/**
  * Command for create hoodie table.
  * @param table
  * @param ignoreIfExists
  */
case class CreateHoodieTableCommand(table: CatalogTable, ignoreIfExists: Boolean)
  extends RunnableCommand with SparkSqlAdapterSupport {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)

    val tableName = table.identifier.unquotedString
    val sessionState = sparkSession.sessionState
    val tableIsExists = sessionState.catalog.tableExists(table.identifier)
    if (tableIsExists) {
      if (ignoreIfExists) {
        // scalastyle:off
        return Seq.empty[Row]
        // scalastyle:on
      } else {
        throw new IllegalArgumentException(s"Table $tableName already exists.")
      }
    }

    var path = getTableLocation(table, sparkSession)
      .getOrElse(s"Missing path for table ${table.identifier}")
    val conf = sparkSession.sessionState.newHadoopConf()
    val isTableExists = tableExists(path, conf)
    // Get the schema & table options
    val (newSchema, tableOptions) = if (table.tableType == CatalogTableType.EXTERNAL &&
      isTableExists) {
      // If this is an external table & the table has already exists in the location,
      // load the schema from the table meta.
      assert(table.schema.isEmpty,
        s"Should not specified table schema for an exists hoodie external " +
          s"table: ${table.identifier.unquotedString}")
      // Get Schema from the external table
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(path)
        .setConf(conf)
        .build()
      val schemaResolver = new TableSchemaResolver(metaClient)
      val avroSchema = schemaResolver.getTableAvroSchema(true)
      val tableSchema = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
      // Get options from the external table
      val options = HoodieOptionConfig.mappingTableConfigToSqlOption(
        metaClient.getTableConfig.getProps.asScala.toMap)
      (tableSchema, options)
    } else {
      assert(table.schema.nonEmpty, s"Missing schema for Create Table: $tableName")
      // SPARK-19724: the default location of a managed table should be non-existent or empty.
      if (table.tableType == CatalogTableType.MANAGED &&
        !sparkSqlAdapter.allowCreatingManagedTableUsingNonemptyLocation(sparkSession.sqlContext.conf)
        && isTableExists) {
        throw new AnalysisException(s"Can not create the managed table('$tableName')" +
            s". The associated location('$path') already exists.")
      }
      // Add the meta fields to the schema if this is a managed table or an empty external table.
      (addMetaFields(table.schema), table.storage.properties)
    }
    // Append the "*" to the path as currently hoodie must specify
    // the same number of STAR as the partition level.
    // TODO Some command e.g. MSCK REPAIR TABLE  will crash when the path contains "*". And it is
    // also not friendly to users to append the "*" to the path. I have file a issue for this
    // to support no start query for hoodie at https://issues.apache.org/jira/browse/HUDI-1591
    val newPath = if (table.partitionColumnNames.nonEmpty) {
      if (path.endsWith("/")) {
        path = path.substring(0, path.length - 1)
      }
      for (_ <- 0 until table.partitionColumnNames.size + 1) {
        path = s"$path/*"
      }
      path
    } else {
      path
    }
    val tableType = HoodieOptionConfig.getTableType(table.storage.properties)
    val inputFormat = tableType match {
      case DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL =>
        classOf[HoodieParquetInputFormat].getCanonicalName
      case DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL =>
        classOf[HoodieParquetRealtimeInputFormat].getCanonicalName
      case _=> throw new IllegalArgumentException(s"UnKnow table type:$tableType")
    }
    val outputFormat = HoodieInputFormatUtils.getOutputFormatClassName(HoodieFileFormat.PARQUET)
    val serdeFormat = HoodieInputFormatUtils.getSerDeClassName(HoodieFileFormat.PARQUET)

    val newStorage = new CatalogStorageFormat(Some(new Path(newPath).toUri),
      Some(inputFormat), Some(outputFormat), Some(serdeFormat),
      table.storage.compressed, tableOptions + ("path" -> newPath))

    val newDatabaseName = formatName(table.identifier.database
      .getOrElse(sessionState.catalog.getCurrentDatabase))
    val newTableName = formatName(table.identifier.table)

    val newTableIdentifier = table.identifier
      .copy(table = newTableName, database = Some(newDatabaseName))

    val newTable = table.copy(identifier = newTableIdentifier,
      schema = newSchema, storage = newStorage, createVersion = SPARK_VERSION)

    val enableHive = "hive" == sessionState.conf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION)
    if (enableHive) {
      createHiveDataSourceTable(newTable, sparkSession)
    } else {
      sessionState.catalog.createTable(newTable, ignoreIfExists = false)
    }
    // Init the hoodie.properties
    initTableIfNeed(sparkSession, newTable)
    Seq.empty[Row]
  }

  /**
    * Create Hive table for hudi.
    * Firstly, do some check for the schema & table.
    * Secondly, append some table properties need for spark datasource table.
    * Thirdly, create hive table using the HiveClient.
    * @param table
    * @param sparkSession
    */
  private def createHiveDataSourceTable(table: CatalogTable, sparkSession: SparkSession): Unit = {
    // check schema
    verifyDataSchema(table.identifier, table.tableType, table.schema)
    val dbName = table.identifier.database.get
    // check database
    val dbExists = sparkSession.sessionState.catalog.databaseExists(dbName)
    if (!dbExists) {
      throw new NoSuchDatabaseException(dbName)
    }
    // check table exists
    if (sparkSession.sessionState.catalog.tableExists(table.identifier)) {
      throw new TableAlreadyExistsException(dbName, table.identifier.table)
    }
    // append some table properties need for spark data source table.
    val dataSourceProps = tableMetaToTableProps(sparkSession.sparkContext.conf,
      table, table.schema)

    val tableWithDataSourceProps = table.copy(properties = dataSourceProps)
    val client = HiveClientUtils.newClientForMetadata(sparkSession.sparkContext.conf,
      sparkSession.sessionState.newHadoopConf())
    // create hive table.
    client.createTable(tableWithDataSourceProps, ignoreIfExists)
  }

  private def formatName(name: String): String = {
    if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
  }

  // This code is forked from org.apache.spark.sql.hive.HiveExternalCatalog#verifyDataSchema
  private def verifyDataSchema(tableName: TableIdentifier,
                               tableType: CatalogTableType,
                               dataSchema: StructType): Unit = {
    if (tableType != CatalogTableType.VIEW) {
      val invalidChars = Seq(",", ":", ";")
      def verifyNestedColumnNames(schema: StructType): Unit = schema.foreach { f =>
        f.dataType match {
          case st: StructType => verifyNestedColumnNames(st)
          case _ if invalidChars.exists(f.name.contains) =>
            val invalidCharsString = invalidChars.map(c => s"'$c'").mkString(", ")
            val errMsg = "Cannot create a table having a nested column whose name contains " +
              s"invalid characters ($invalidCharsString) in Hive metastore. Table: $tableName; " +
              s"Column: ${f.name}"
            throw new AnalysisException(errMsg)
          case _ =>
        }
      }

      dataSchema.foreach { f =>
        f.dataType match {
          // Checks top-level column names
          case _ if f.name.contains(",") =>
            throw new AnalysisException("Cannot create a table having a column whose name " +
              s"contains commas in Hive metastore. Table: $tableName; Column: ${f.name}")
          // Checks nested column names
          case st: StructType =>
            verifyNestedColumnNames(st)
          case _ =>
        }
      }
    }
  }
  // This code is forked from org.apache.spark.sql.hive.HiveExternalCatalog#tableMetaToTableProps
  private def tableMetaToTableProps( sparkConf: SparkConf,
                                     table: CatalogTable,
                                     schema: StructType): Map[String, String] = {
    val partitionColumns = table.partitionColumnNames
    val bucketSpec = table.bucketSpec

    val properties = new mutable.HashMap[String, String]
    properties.put(DATASOURCE_PROVIDER, "hudi")
    properties.put(CREATED_SPARK_VERSION, table.createVersion)

    // Serialized JSON schema string may be too long to be stored into a single metastore table
    // property. In this case, we split the JSON string and store each part as a separate table
    // property.
    val threshold = sparkConf.get(SCHEMA_STRING_LENGTH_THRESHOLD)
    val schemaJsonString = schema.json
    // Split the JSON string.
    val parts = schemaJsonString.grouped(threshold).toSeq
    properties.put(DATASOURCE_SCHEMA_NUMPARTS, parts.size.toString)
    parts.zipWithIndex.foreach { case (part, index) =>
      properties.put(s"$DATASOURCE_SCHEMA_PART_PREFIX$index", part)
    }

    if (partitionColumns.nonEmpty) {
      properties.put(DATASOURCE_SCHEMA_NUMPARTCOLS, partitionColumns.length.toString)
      partitionColumns.zipWithIndex.foreach { case (partCol, index) =>
        properties.put(s"$DATASOURCE_SCHEMA_PARTCOL_PREFIX$index", partCol)
      }
    }

    if (bucketSpec.isDefined) {
      val BucketSpec(numBuckets, bucketColumnNames, sortColumnNames) = bucketSpec.get

      properties.put(DATASOURCE_SCHEMA_NUMBUCKETS, numBuckets.toString)
      properties.put(DATASOURCE_SCHEMA_NUMBUCKETCOLS, bucketColumnNames.length.toString)
      bucketColumnNames.zipWithIndex.foreach { case (bucketCol, index) =>
        properties.put(s"$DATASOURCE_SCHEMA_BUCKETCOL_PREFIX$index", bucketCol)
      }

      if (sortColumnNames.nonEmpty) {
        properties.put(DATASOURCE_SCHEMA_NUMSORTCOLS, sortColumnNames.length.toString)
        sortColumnNames.zipWithIndex.foreach { case (sortCol, index) =>
          properties.put(s"$DATASOURCE_SCHEMA_SORTCOL_PREFIX$index", sortCol)
        }
      }
    }

    properties.toMap
  }
}

object CreateHoodieTableCommand extends Logging {

  /**
    * Init the table if it is not exists.
    * @param sparkSession
    * @param table
    * @return
    */
  def initTableIfNeed(sparkSession: SparkSession, table: CatalogTable): Unit = {
    val location = removeStarFromPath(getTableLocation(table, sparkSession).getOrElse(
      throw new IllegalArgumentException(s"Missing location for ${table.identifier}")))

    val conf = sparkSession.sessionState.newHadoopConf()
    // Init the hoodie table
    if (!tableExists(location, conf)) {
      val tableName = table.identifier.table
      logInfo(s"Table $tableName is not exists, start to create the hudi table")

      // Save all the table config to the hoodie.properties.
      val parameters = HoodieOptionConfig.mappingSqlOptionToTableConfig(table.storage.properties)
      val properties = new Properties()
      properties.putAll(parameters.asJava)
      HoodieTableMetaClient.withPropertyBuilder()
          .fromProperties(properties)
          .setTableName(tableName)
          .setTableSchema(SchemaConverters.toAvroType(table.schema).toString())
          .initTable(conf, location)
    }
  }

  def tableExists(tablePath: String, conf: Configuration): Boolean = {
    val basePath = new Path(tablePath)
    val fs = basePath.getFileSystem(conf)
    val metaPath = new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME)
    fs.exists(metaPath)
  }
}
