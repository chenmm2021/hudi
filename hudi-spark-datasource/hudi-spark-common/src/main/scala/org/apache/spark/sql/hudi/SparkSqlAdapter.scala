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

import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.internal.SQLConf

/**
  * An interface to adapter the difference between spark2 and spark3
  * in some spark sql related class.
  */
trait SparkSqlAdapter {

  def createSparkRowSerDe(encoder: ExpressionEncoder[Row]): SparkRowSerDe

  def allowCreatingManagedTableUsingNonemptyLocation(conf: SQLConf): Boolean

  def toTableIdentify(aliasId: AliasIdentifier): TableIdentifier

  def toTableIdentify(relation: UnresolvedRelation): TableIdentifier

  def createJoin(left: LogicalPlan, right: LogicalPlan, joinType: JoinType): Join

  def isInsertInto(plan: LogicalPlan): Boolean

  def getInsertIntoChildren(plan: LogicalPlan):
    Option[(LogicalPlan, Map[String, Option[String]], LogicalPlan, Boolean, Boolean)]

  def createInsertInto(table: LogicalPlan, partition: Map[String, Option[String]],
    query: LogicalPlan, overwrite: Boolean, ifPartitionNotExists: Boolean): LogicalPlan

  def createExtendedSparkParser: Option[(SparkSession, ParserInterface) => ParserInterface] = None

}
