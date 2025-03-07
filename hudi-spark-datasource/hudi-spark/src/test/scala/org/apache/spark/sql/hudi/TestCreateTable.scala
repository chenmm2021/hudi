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

package org.apache.spark.sql.hudi

import scala.collection.JavaConverters._
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField}

class TestCreateTable extends TestHoodieSqlBase {

  test("Test Create Managed Hoodie Table") {
    val tableName = generateTableName
    // Create a managed table
    spark.sql(
      s"""
         | create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         | ) using hudi
         | options (
         |   primaryKey = 'id',
         |   versionColumn = 'ts'
         | )
       """.stripMargin)
    val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
    assertResult(tableName)(table.identifier.table)
    assertResult("hudi")(table.provider.get)
    assertResult(CatalogTableType.MANAGED)(table.tableType)
    assertResult(
      HoodieRecord.HOODIE_META_COLUMNS.asScala.map(StructField(_, StringType))
        ++ Seq(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("price", DoubleType),
        StructField("ts", LongType))
    )(table.schema.fields)
  }

  test("Test Create External Hoodie Table") {
    withTempDir { tmp =>
      // Test create cow table.
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | options (
           |  primaryKey = 'id,name',
           |  type = 'cow'
           | )
           | location '${tmp.getCanonicalPath}'
       """.stripMargin)
      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))

      assertResult(tableName)(table.identifier.table)
      assertResult("hudi")(table.provider.get)
      assertResult(CatalogTableType.EXTERNAL)(table.tableType)
      assertResult(
        HoodieRecord.HOODIE_META_COLUMNS.asScala.map(StructField(_, StringType))
          ++ Seq(
          StructField("id", IntegerType),
          StructField("name", StringType),
          StructField("price", DoubleType),
          StructField("ts", LongType))
      )(table.schema.fields)
      assertResult(table.storage.properties("type"))("cow")
      assertResult(table.storage.properties("primaryKey"))("id,name")

      spark.sql(s"drop table $tableName")
      // Test create mor partitioned table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           |) using hudi
           | partitioned by (dt)
           | options (
           |  primaryKey = 'id',
           |  type = 'mor'
           | )
           | location '${tmp.getCanonicalPath}/h0'
       """.stripMargin)
      val table2 = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      assertResult(table2.storage.properties("type"))("mor")
      assertResult(table2.storage.properties("primaryKey"))("id")
      assertResult(Seq("dt"))(table2.partitionColumnNames)
      assertResult(classOf[HoodieParquetRealtimeInputFormat].getCanonicalName)(table2.storage.inputFormat.get)

      // Test create a external table with an exist table in the path
      val tableName3 = generateTableName
      spark.sql(
        s"""
           |create table $tableName3
           |using hudi
           |location '${tmp.getCanonicalPath}/h0'
         """.stripMargin)
      val table3 = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName3))
      assertResult(table3.storage.properties("type"))("mor")
      assertResult(table3.storage.properties("primaryKey"))("id")
      assertResult(
        HoodieRecord.HOODIE_META_COLUMNS.asScala.map(StructField(_, StringType))
          ++ Seq(
          StructField("id", IntegerType),
          StructField("name", StringType),
          StructField("price", DoubleType),
          StructField("ts", LongType),
          StructField("dt", StringType)
        )
      )(table3.schema.fields)
    }
  }

  test("Test Create Table As Select") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName using hudi
           | location '${tmp.getCanonicalPath}'
           | AS
           | select 1 as id, 'a1' as name, 10 as price, 1000 as ts
       """.stripMargin)
      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a1", 10.0, 1000)
      )
    }
  }
}
