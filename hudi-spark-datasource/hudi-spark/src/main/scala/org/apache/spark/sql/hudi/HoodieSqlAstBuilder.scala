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
// scalastyle:off
package org.apache.spark.sql.hudi

import java.sql.{Date, Timestamp}
import java.util.Locale

import javax.xml.bind.DatatypeConverter

import scala.collection.JavaConverters._
import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.tree.{ParseTree, RuleNode, TerminalNode}
import org.apache.hudi.spark.sql.parser.{HoodieSqlBaseBaseVisitor, HoodieSqlBaseParser}
import org.apache.hudi.spark.sql.parser.HoodieSqlBaseParser.{AMPERSAND, AND, ARRAY, ASTERISK, AggregationContext, AliasedQueryContext, AliasedRelationContext, ArithmeticBinaryContext, ArithmeticUnaryContext, AssignmentListContext, BETWEEN, BOTH, BigDecimalLiteralContext, BigIntLiteralContext, BooleanExpressionContext, BooleanLiteralContext, BucketSpecContext, CONCAT_PIPE, CURRENT, CastContext, ColTypeContext, ColTypeListContext, ColumnReferenceContext, ComparisonContext, ComplexColTypeContext, ComplexColTypeListContext, ComplexDataTypeContext, CreateTableHeaderContext, CreateTableWithPkContext, DISTINCT, DIV, DataTypeContext, DecimalLiteralContext, DeleteTableContext, DereferenceContext, DoubleLiteralContext, EQ, ExistsContext, ExpressionContext, ExtractContext, FOLLOWING, FirstContext, FrameBoundContext, FromClauseContext, FunctionCallContext, GT, GTE, HAT, HintContext, IN, IdentifierListContext, IdentifierSeqContext, InlineTableContext, IntegerLiteralContext, IntervalContext, IntervalFieldContext, LEADING, LIKE, LT, LTE, LambdaContext, LastContext, LateralViewContext, LocationSpecContext, LogicalBinaryContext, LogicalNotContext, MAP, MINUS, MergeIntoContext, MergeIntoTableContext, NEQ, NEQJ, NSEQ, NULL, NamedExpressionContext, NamedQueryContext, NullLiteralContext, NumberContext, OR, PERCENT, PIPE, PLUS, PRECEDING, ParenthesizedExpressionContext, PivotClauseContext, PivotValueContext, PositionContext, PredicateContext, PredicatedContext, PrimitiveDataTypeContext, QualifiedNameContext, QueryContext, QueryOrganizationContext, QuerySpecificationContext, RANGE, RLIKE, ROWS, RelationContext, RowConstructorContext, RowFormatContext, RowFormatDelimitedContext, RowFormatSerdeContext, SLASH, STRUCT, SampleByBucketContext, SampleByBytesContext, SampleByPercentileContext, SampleByRowsContext, SampleContext, SearchedCaseContext, SetOperationContext, SimpleCaseContext, SingleInsertQueryContext, SingleStatementContext, SmallIntLiteralContext, SortItemContext, StarContext, StringLiteralContext, StructContext, SubqueryContext, SubqueryExpressionContext, SubscriptContext, TILDE, TRAILING, TableAliasContext, TableContext, TableIdentifierContext, TableNameContext, TablePropertyKeyContext, TablePropertyListContext, TablePropertyValueContext, TableValuedFunctionContext, TinyIntLiteralContext, TypeConstructorContext, UpdateTableContext, WindowDefContext, WindowRefContext, WindowsContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{MultiAlias, UnresolvedAlias, UnresolvedAttribute, UnresolvedExtractValue, UnresolvedFunction, UnresolvedGenerator, UnresolvedInlineTable, UnresolvedRegex, UnresolvedRelation, UnresolvedStar, UnresolvedSubqueryColumnAliases, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, And, Ascending, AttributeReference, BitwiseAnd, BitwiseNot, BitwiseOr, BitwiseXor, CaseWhen, Cast, Concat, CreateNamedStruct, CreateStruct, Cube, CurrentRow, DayOfMonth, DayOfWeek, Descending, Divide, EqualNullSafe, EqualTo, Exists, Expression, GreaterThan, GreaterThanOrEqual, Hour, In, InSubquery, IsNotNull, IsNull, LambdaFunction, LessThan, LessThanOrEqual, Like, ListQuery, Literal, Minute, Month, Multiply, NamedExpression, Not, NullsFirst, NullsLast, Or, Predicate, Quarter, RLike, RangeFrame, Remainder, Rollup, RowFrame, ScalarSubquery, Second, SortOrder, SpecifiedWindowFrame, StringLocate, Subtract, UnaryMinus, UnboundedFollowing, UnboundedPreceding, UnresolvedNamedLambdaVariable, UnresolvedWindowExpression, UnspecifiedFrame, WeekOfYear, WindowExpression, WindowSpec, WindowSpecDefinition, WindowSpecReference, Year}
import org.apache.spark.sql.catalyst.expressions.aggregate.{First, Last}
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserUtils}
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, NaturalJoin, RightOuter, UsingJoin}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Distinct, Except, Filter, Generate, GroupingSets, Intersect, Join, Limit, LogicalPlan, OneRowRelation, Pivot, Project, RepartitionByExpression, Sample, ScriptInputOutputSchema, ScriptTransformation, Sort, SubqueryAlias, Union, UnresolvedHint, With, WithWindowDefinition}
import org.apache.spark.sql.execution.datasources.{CreateTable, CreateTempViewUsing, DataSource}
import org.apache.spark.sql.hudi.logical.{Assignment, DeleteAction, DeleteTable, InsertAction, MergeAction, MergeIntoTable, UpdateAction, UpdateTable}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, CharType, DataType, DateType, DecimalType, DoubleType, FloatType, HIVE_TYPE_STRING, HiveStringType, IntegerType, LongType, MapType, MetadataBuilder, ShortType, StringType, StructField, StructType, TimestampType, VarcharType}
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.random.RandomSampler

import scala.collection.mutable.ArrayBuffer

/**
  * The AstBuilder for hoodie. Most the code is forked from
  * org.apache.spark.sql.catalyst.parser.AstBuilder and base on
  * that, we append our logical for convert extended syntax tree to LogicalPlan.
  * TODO Find the better way to reused the code from spark AstBuilder, Not by copy code.
  *
  * @param conf
  */
class HoodieSqlAstBuilder(conf: SQLConf) extends HoodieSqlBaseBaseVisitor[AnyRef] with Logging {

  import ParserUtils._

  override def visitMergeIntoTable (ctx: MergeIntoTableContext): LogicalPlan = withOrigin(ctx) {
    visitMergeInto(ctx.mergeInto())
  }

  override def visitMergeInto(ctx: MergeIntoContext): LogicalPlan = withOrigin(ctx) {
    val target = UnresolvedRelation(visitTableIdentifier(ctx.target))
    val source = if (ctx.source != null) {
      UnresolvedRelation(visitTableIdentifier(ctx.source))
    } else {
      visitQuery(ctx.subquery)
    }
    val aliasedTarget =
      if (ctx.tableAlias(0) != null) mayApplyAliasPlan(ctx.tableAlias(0), target) else target
    val aliasedSource =
      if (ctx.tableAlias(1) != null) mayApplyAliasPlan(ctx.tableAlias(1), source) else source

    val mergeCondition = expression(ctx.mergeCondition().condition)

    if (ctx.matchedClauses().size() > 2) {
      throw new ParseException("There should be at most 2 'WHEN MATCHED' clauses.",
        ctx.matchedClauses.get(2))
    }

    val matchedClauses: Seq[MergeAction] = ctx.matchedClauses().asScala.flatMap {
      c =>
        val deleteCtx = c.deleteClause()
        val deleteClause = if (deleteCtx != null) {
          val deleteCond = if (deleteCtx.deleteCond != null) {
            Some(expression(deleteCtx.deleteCond))
          } else {
            None
          }
          Some(DeleteAction(deleteCond))
        } else {
          None
        }
        val updateCtx = c.updateClause()
        val updateClause = if (updateCtx != null) {
          val updateAction = updateCtx.updateAction()
          val updateCond = if (updateCtx.updateCond != null) {
            Some(expression(updateCtx.updateCond))
          } else {
            None
          }
          if (updateAction.ASTERISK() != null) {
            Some(UpdateAction(updateCond, Seq.empty))
          } else {
            val assignments = withAssignments(updateAction.assignmentList())
            Some(UpdateAction(updateCond, assignments))
          }
        } else {
          None
        }
        (deleteClause ++ updateClause).toSeq
    }
    val notMatchedClauses: Seq[InsertAction] = ctx.notMatchedClause().asScala.map {
      notMatchedClause =>
        val insertCtx = notMatchedClause.insertClause()
        val insertAction = insertCtx.insertAction()
        val insertCond = if (insertCtx.insertCond != null) {
          Some(expression(insertCtx.insertCond))
        } else {
          None
        }
        if (insertAction.ASTERISK() != null) {
          InsertAction(insertCond, Seq.empty)
        } else {
          val attrList = insertAction.columns.qualifiedName().asScala
            .map(attr => UnresolvedAttribute(visitQualifiedName(attr)))
          val attrSet = scala.collection.mutable.Set[UnresolvedAttribute]()
          attrList.foreach(attr => {
            if (attrSet.contains(attr)) {
              throw new ParseException(s"find duplicate field :'${attr.name}'",
                insertAction.columns)
            }
            attrSet += attr
          })
          val valueList = insertAction.expression().asScala.map(expression)
          if (attrList.size != valueList.size) {
            throw new ParseException("The columns of source and target tables are not equal: " +
              s"target: $attrList, source: $valueList", insertAction)
          }
          val assignments = attrList.zip(valueList).map(kv => Assignment(kv._1, kv._2))
          InsertAction(insertCond, assignments)
        }
    }
    MergeIntoTable(aliasedTarget, aliasedSource, mergeCondition,
      matchedClauses, notMatchedClauses)
  }

  private def withAssignments(assignCtx: AssignmentListContext): Seq[Assignment] =
    withOrigin(assignCtx) {
      assignCtx.assignment().asScala.map { assign =>
        Assignment(UnresolvedAttribute(visitQualifiedName(assign.key)),
          expression(assign.value))
      }
    }

  override def visitCreateTableWithPk (ctx: CreateTableWithPkContext): LogicalPlan =
    withOrigin(ctx) {
      val _ctx = ctx.createTableWithPrimayKey()
      val (table, temp, ifNotExists, external) = visitCreateTableHeader(_ctx.createTableHeader())
      if (external) {
        operationNotAllowed("CREATE EXTERNAL TABLE ... USING", ctx)
      }

      checkDuplicateClauses(_ctx.TBLPROPERTIES, "TBLPROPERTIES", ctx)
      checkDuplicateClauses(_ctx.OPTIONS, "OPTIONS", ctx)
      checkDuplicateClauses(_ctx.PARTITIONED, "PARTITIONED BY", ctx)
      checkDuplicateClauses(_ctx.COMMENT, "COMMENT", ctx)
      checkDuplicateClauses(_ctx.bucketSpec(), "CLUSTERED BY", ctx)
      checkDuplicateClauses(_ctx.locationSpec, "LOCATION", ctx)

      val primaryKeyColumns = if (_ctx.primaryKey() != null) {
        _ctx.primaryKey().identifier().asScala.map(_.getText)
      } else {
        Seq.empty[String]
      }
      // we store the pk info to the table options
      val options = Option(_ctx.options).map(visitPropertyKeyValues)
        .getOrElse(Map.empty) + (SQLConfig.SQL_TABLE_PRIMARY_KEY -> primaryKeyColumns.mkString(","))
      val provider = _ctx.tableProvider.qualifiedName.getText
      val schema = Option(_ctx.colTypeList()).map(createSchema)
      val partitionColumnNames =
        Option(_ctx.partitionColumnNames)
          .map(visitIdentifierList(_).toArray)
          .getOrElse(Array.empty[String])

      val properties = Option(_ctx.tableProps).map(visitPropertyKeyValues).getOrElse(Map.empty)

      val bucketSpec = _ctx.bucketSpec().asScala.headOption.map(visitBucketSpec)

      val location = _ctx.locationSpec.asScala.headOption.map(visitLocationSpec)
      val storage = DataSource.buildStorageFormatFromOptions(options)

      if (location.isDefined && storage.locationUri.isDefined) {
        throw new ParseException(
          "LOCATION and 'path' in OPTIONS are both used to indicate the custom table path, " +
            "you can only specify one of them.", ctx)
      }
      val customLocation = storage.locationUri.orElse(location.map(CatalogUtils.stringToURI))

      val tableType = if (customLocation.isDefined) {
        CatalogTableType.EXTERNAL
      } else {
        CatalogTableType.MANAGED
      }

      val tableDesc = CatalogTable(
        identifier = table,
        tableType = tableType,
        storage = storage.copy(locationUri = customLocation),
        schema = schema.getOrElse(new StructType),
        provider = Some(provider),
        partitionColumnNames = partitionColumnNames,
        bucketSpec = bucketSpec,
        properties = properties,
        comment = Option(_ctx.comment).map(string))

      // Determine the storage mode.
      val mode = if (ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists

      if (_ctx.query != null) {
        // Get the backing query.
        val query = plan(_ctx.query)

        if (temp) {
          operationNotAllowed("CREATE TEMPORARY TABLE ... USING ... AS query", ctx)
        }

        // Don't allow explicit specification of schema for CTAS
        if (schema.nonEmpty) {
          operationNotAllowed(
            "Schema may not be specified in a Create Table As Select (CTAS) statement",
            ctx)
        }
        CreateTable(tableDesc, mode, Some(query))
      } else {
        if (temp) {
          if (ifNotExists) {
            operationNotAllowed("CREATE TEMPORARY TABLE IF NOT EXISTS", ctx)
          }

          logWarning(s"CREATE TEMPORARY TABLE ... USING ... is deprecated, please use " +
            "CREATE TEMPORARY VIEW ... USING ... instead")
          // Unlike CREATE TEMPORARY VIEW USING, CREATE TEMPORARY TABLE USING does not support
          // IF NOT EXISTS. Users are not allowed to replace the existing temp table.
          CreateTempViewUsing(table, schema, replace = false, global = false, provider, options)
        } else {
          CreateTable(tableDesc, mode, None)
        }
      }
    }

  override def visitUpdateTable(ctx: UpdateTableContext): LogicalPlan = withOrigin(ctx) {
    val updateStmt = ctx.updateTableStmt()
    val table = UnresolvedRelation(visitTableIdentifier(updateStmt.tableIdentifier()))
    val condition = if (updateStmt.where != null) Some(expression(updateStmt.where)) else None
    val assignments = withAssignments(updateStmt.assignmentList())
    UpdateTable(table, condition, assignments)
  }

  override def visitDeleteTable (ctx: DeleteTableContext): LogicalPlan = withOrigin(ctx) {
    val deleteStmt = ctx.deleteTableStmt()
    val table = UnresolvedRelation(visitTableIdentifier(deleteStmt.tableIdentifier()))
    val condition = if (deleteStmt.where != null) Some(expression(deleteStmt.where)) else None
    DeleteTable(table, condition)
  }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }
  /**
    * Type to keep track of a table header: (identifier, isTemporary, ifNotExists, isExternal).
    */
  type TableHeader = (TableIdentifier, Boolean, Boolean, Boolean)

  /**
    * Validate a create table statement and return the [[TableIdentifier]].
    */
  override def visitCreateTableHeader(ctx: CreateTableHeaderContext): TableHeader = withOrigin(ctx) {
    val temporary = ctx.TEMPORARY != null
    val ifNotExists = ctx.EXISTS != null
    if (temporary && ifNotExists) {
      operationNotAllowed("CREATE TEMPORARY TABLE ... IF NOT EXISTS", ctx)
    }
    (visitTableIdentifier(ctx.tableIdentifier), temporary, ifNotExists, ctx.EXTERNAL != null)
  }

  /**
    * Convert a table property list into a key-value map.
    * This should be called through [[visitPropertyKeyValues]] or [[visitPropertyKeys]].
    */
  override def visitTablePropertyList(
                                       ctx: TablePropertyListContext): Map[String, String] = withOrigin(ctx) {
    val properties = ctx.tableProperty.asScala.map { property =>
      val key = visitTablePropertyKey(property.key)
      val value = visitTablePropertyValue(property.value)
      key -> value
    }
    // Check for duplicate property names.
    checkDuplicateKeys(properties, ctx)
    properties.toMap
  }

  /**
    * Parse a key-value map from a [[TablePropertyListContext]], assuming all values are specified.
    */
  private def visitPropertyKeyValues(ctx: TablePropertyListContext): Map[String, String] = {
    val props = visitTablePropertyList(ctx)
    val badKeys = props.collect { case (key, null) => key }
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values must be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx)
    }
    props
  }

  /**
    * Parse a list of keys from a [[TablePropertyListContext]], assuming no values are specified.
    */
  private def visitPropertyKeys(ctx: TablePropertyListContext): Seq[String] = {
    val props = visitTablePropertyList(ctx)
    val badKeys = props.filter { case (_, v) => v != null }.keys
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values should not be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx)
    }
    props.keys.toSeq
  }

  /**
    * A table property key can either be String or a collection of dot separated elements. This
    * function extracts the property key based on whether its a string literal or a table property
    * identifier.
    */
  override def visitTablePropertyKey(key: TablePropertyKeyContext): String = {
    if (key.STRING != null) {
      string(key.STRING)
    } else {
      key.getText
    }
  }

  /**
    * A table property value can be String, Integer, Boolean or Decimal. This function extracts
    * the property value based on whether its a string, integer, boolean or decimal literal.
    */
  override def visitTablePropertyValue(value: TablePropertyValueContext): String = {
    if (value == null) {
      null
    } else if (value.STRING != null) {
      string(value.STRING)
    } else if (value.booleanValue != null) {
      value.getText.toLowerCase(Locale.ROOT)
    } else {
      value.getText
    }
  }

  /**
    * Create location string.
    */
  override def visitLocationSpec(ctx: LocationSpecContext): String = withOrigin(ctx) {
    string(ctx.STRING)
  }

  /**
    * Create a [[BucketSpec]].
    */
  override def visitBucketSpec(ctx: BucketSpecContext): BucketSpec =
    withOrigin(ctx) {
      BucketSpec(
        ctx.INTEGER_VALUE.getText.toInt,
        visitIdentifierList(ctx.identifierList),
        Option(ctx.orderedIdentifierList)
          .toSeq
          .flatMap(_.orderedIdentifier.asScala)
          .map { orderedIdCtx =>
            Option(orderedIdCtx.ordering).map(_.getText).foreach { dir =>
              if (dir.toLowerCase(Locale.ROOT) != "asc") {
                operationNotAllowed(s"Column ordering must be ASC, was '$dir'", ctx)
              }
            }

            orderedIdCtx.identifier.getText
          })
    }

  /* ********************************************************************************************
   * Plan parsing
   * ******************************************************************************************** */
  private def plan(tree: ParserRuleContext): LogicalPlan = typedVisit(tree)

  /**
    * Override the default behavior for all visit methods. This will only return a non-null result
    * when the context has only one child. This is done because there is no generic method to
    * combine the results of the context children. In all other cases null is returned.
    */
  override def visitChildren(node: RuleNode): AnyRef = {
    if (node.getChildCount == 1) {
      node.getChild(0).accept(this)
    } else {
      null
    }
  }

  private def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }

  /**
    * Create a logical plan for a regular (single-insert) query.
    */
  override def visitSingleInsertQuery(ctx: SingleInsertQueryContext): LogicalPlan = withOrigin(ctx) {
    plan(ctx.queryTerm).
      // Add organization statements.
      optionalMap(ctx.queryOrganization)(withQueryResultClauses)
  }

  /**
    * Add ORDER BY/SORT BY/CLUSTER BY/DISTRIBUTE BY/LIMIT/WINDOWS clauses to the logical plan. These
    * clauses determine the shape (ordering/partitioning/rows) of the query result.
    */
  private def withQueryResultClauses(
                                      ctx: QueryOrganizationContext,
                                      query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    import ctx._

    // Handle ORDER BY, SORT BY, DISTRIBUTE BY, and CLUSTER BY clause.
    val withOrder = if (
      !order.isEmpty && sort.isEmpty && distributeBy.isEmpty && clusterBy.isEmpty) {
      // ORDER BY ...
      Sort(order.asScala.map(visitSortItem), global = true, query)
    } else if (order.isEmpty && !sort.isEmpty && distributeBy.isEmpty && clusterBy.isEmpty) {
      // SORT BY ...
      Sort(sort.asScala.map(visitSortItem), global = false, query)
    } else if (order.isEmpty && sort.isEmpty && !distributeBy.isEmpty && clusterBy.isEmpty) {
      // DISTRIBUTE BY ...
      withRepartitionByExpression(ctx, expressionList(distributeBy), query)
    } else if (order.isEmpty && !sort.isEmpty && !distributeBy.isEmpty && clusterBy.isEmpty) {
      // SORT BY ... DISTRIBUTE BY ...
      Sort(
        sort.asScala.map(visitSortItem),
        global = false,
        withRepartitionByExpression(ctx, expressionList(distributeBy), query))
    } else if (order.isEmpty && sort.isEmpty && distributeBy.isEmpty && !clusterBy.isEmpty) {
      // CLUSTER BY ...
      val expressions = expressionList(clusterBy)
      Sort(
        expressions.map(SortOrder(_, Ascending)),
        global = false,
        withRepartitionByExpression(ctx, expressions, query))
    } else if (order.isEmpty && sort.isEmpty && distributeBy.isEmpty && clusterBy.isEmpty) {
      // [EMPTY]
      query
    } else {
      throw new ParseException(
        "Combination of ORDER BY/SORT BY/DISTRIBUTE BY/CLUSTER BY is not supported", ctx)
    }

    // WINDOWS
    val withWindow = withOrder.optionalMap(windows)(withWindows)

    // LIMIT
    // - LIMIT ALL is the same as omitting the LIMIT clause
    withWindow.optional(limit) {
      Limit(typedVisit(limit), withWindow)
    }
  }

  /**
    * Add a [[WithWindowDefinition]] operator to a logical plan.
    */
  private def withWindows(ctx: WindowsContext,
                          query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    // Collect all window specifications defined in the WINDOW clause.
    val baseWindowMap = ctx.namedWindow.asScala.map {
      wCtx =>
        (wCtx.identifier.getText, typedVisit[WindowSpec](wCtx.windowSpec))
    }.toMap

    // Handle cases like
    // window w1 as (partition by p_mfgr order by p_name
    //               range between 2 preceding and 2 following),
    //        w2 as w1
    val windowMapView = baseWindowMap.mapValues {
      case WindowSpecReference(name) =>
        baseWindowMap.get(name) match {
          case Some(spec: WindowSpecDefinition) =>
            spec
          case Some(ref) =>
            throw new ParseException(s"Window reference '$name' is not a window specification", ctx)
          case None =>
            throw new ParseException(s"Cannot resolve window reference '$name'", ctx)
        }
      case spec: WindowSpecDefinition => spec
    }

    // Note that mapValues creates a view instead of materialized map. We force materialization by
    // mapping over identity.
    WithWindowDefinition(windowMapView.map(identity), query)
  }

  /**
    * Create a clause for DISTRIBUTE BY.
    */
  protected def withRepartitionByExpression(
                                             ctx: QueryOrganizationContext,
                                             expressions: Seq[Expression],
                                             query: LogicalPlan): LogicalPlan = {
    RepartitionByExpression(expressions, query, conf.numShufflePartitions)
  }

  /**
    * Add [[org.apache.spark.sql.catalyst.plans.logical.UnresolvedHint]]s to a logical plan.
    */
  private def withHints(
                         ctx: HintContext,
                         query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    var plan = query
    ctx.hintStatements.asScala.reverse.foreach { case stmt =>
      plan = UnresolvedHint(stmt.hintName.getText, stmt.parameters.asScala.map(expression), plan)
    }
    plan
  }

  /**
    * Add a [[org.apache.spark.sql.catalyst.plans.logical.Pivot]] to a logical plan.
    */
  private def withPivot(ctx: PivotClauseContext,
                        query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    val aggregates = Option(ctx.aggregates).toSeq
      .flatMap(_.namedExpression.asScala)
      .map(typedVisit[Expression])
    val pivotColumn = if (ctx.pivotColumn.identifiers.size == 1) {
      UnresolvedAttribute.quoted(ctx.pivotColumn.identifier.getText)
    } else {
      CreateStruct(
        ctx.pivotColumn.identifiers.asScala.map(
          identifier => UnresolvedAttribute.quoted(identifier.getText)))
    }
    val pivotValues = ctx.pivotValues.asScala.map(visitPivotValue)
    Pivot(None, pivotColumn, pivotValues, aggregates, query)
  }

  /**
    * Create a logical plan using a query specification.
    */
  override def visitQuerySpecification(
                                        ctx: QuerySpecificationContext): LogicalPlan = withOrigin(ctx) {
    val from = OneRowRelation().optional(ctx.fromClause) {
      visitFromClause(ctx.fromClause)
    }
    withQuerySpecification(ctx, from)
  }

  /**
    * Add a query specification to a logical plan. The query specification is the core of the logical
    * plan, this is where sourcing (FROM clause), transforming (SELECT TRANSFORM/MAP/REDUCE),
    * projection (SELECT), aggregation (GROUP BY ... HAVING ...) and filtering (WHERE) takes place.
    *
    * Note that query hints are ignored (both by the parser and the builder).
    */
  private def withQuerySpecification(ctx: QuerySpecificationContext,
                                     relation: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    import ctx._

    // WHERE
    def filter(ctx: BooleanExpressionContext, plan: LogicalPlan): LogicalPlan = {
      Filter(expression(ctx), plan)
    }

    def withHaving(ctx: BooleanExpressionContext, plan: LogicalPlan): LogicalPlan = {
      // Note that we add a cast to non-predicate expressions. If the expression itself is
      // already boolean, the optimizer will get rid of the unnecessary cast.
      val predicate = expression(ctx) match {
        case p: Predicate => p
        case e => Cast(e, BooleanType)
      }
      Filter(predicate, plan)
    }


    // Expressions.
    val expressions = Option(namedExpressionSeq).toSeq
      .flatMap(_.namedExpression.asScala)
      .map(typedVisit[Expression])

    // Create either a transform or a regular query.
    val specType = Option(kind).map(_.getType).getOrElse(SELECT)
    specType match {
      case HoodieSqlBaseParser.MAP | HoodieSqlBaseParser.REDUCE | HoodieSqlBaseParser.TRANSFORM =>
        // Transform

        // Add where.
        val withFilter = relation.optionalMap(where)(filter)

        // Create the attributes.
        val (attributes, schemaLess) = if (colTypeList != null) {
          // Typed return columns.
          (createSchema(colTypeList).toAttributes, false)
        } else if (identifierSeq != null) {
          // Untyped return columns.
          val attrs = visitIdentifierSeq(identifierSeq).map { name =>
            AttributeReference(name, StringType, nullable = true)()
          }
          (attrs, false)
        } else {
          (Seq(AttributeReference("key", StringType)(),
            AttributeReference("value", StringType)()), true)
        }

        // Create the transform.
        ScriptTransformation(
          expressions,
          string(script),
          attributes,
          withFilter,
          withScriptIOSchema(
            ctx, inRowFormat, recordWriter, outRowFormat, recordReader, schemaLess))

      case HoodieSqlBaseParser.SELECT =>
        // Regular select

        // Add lateral views.
        val withLateralView = ctx.lateralView.asScala.foldLeft(relation)(withGenerate)

        // Add where.
        val withFilter = withLateralView.optionalMap(where)(filter)

        // Add aggregation or a project.
        val namedExpressions = expressions.map {
          case e: NamedExpression => e
          case e: Expression => UnresolvedAlias(e)
        }

        def createProject() = if (namedExpressions.nonEmpty) {
          Project(namedExpressions, withFilter)
        } else {
          withFilter
        }

        val withProject = if (aggregation == null && having != null) {
          if (conf.getConf(SQLConf.LEGACY_HAVING_WITHOUT_GROUP_BY_AS_WHERE)) {
            // If the legacy conf is set, treat HAVING without GROUP BY as WHERE.
            withHaving(having, createProject())
          } else {
            // According to SQL standard, HAVING without GROUP BY means global aggregate.
            withHaving(having, Aggregate(Nil, namedExpressions, withFilter))
          }
        } else if (aggregation != null) {
          val aggregate = withAggregation(aggregation, namedExpressions, withFilter)
          aggregate.optionalMap(having)(withHaving)
        } else {
          // When hitting this branch, `having` must be null.
          createProject()
        }

        // Distinct
        val withDistinct = if (setQuantifier() != null && setQuantifier().DISTINCT() != null) {
          Distinct(withProject)
        } else {
          withProject
        }

        // Window
        val withWindow = withDistinct.optionalMap(windows)(withWindows)

        // Hint
        hints.asScala.foldRight(withWindow)(withHints)
    }
  }

  /**
    * Connect two queries by a Set operator.
    *
    * Supported Set operators are:
    * - UNION [ DISTINCT | ALL ]
    * - EXCEPT [ DISTINCT | ALL ]
    * - MINUS [ DISTINCT | ALL ]
    * - INTERSECT [DISTINCT | ALL]
    */
  override def visitSetOperation(ctx: SetOperationContext): LogicalPlan = withOrigin(ctx) {
    val left = plan(ctx.left)
    val right = plan(ctx.right)
    val all = Option(ctx.setQuantifier()).exists(_.ALL != null)
    ctx.operator.getType match {
      case HoodieSqlBaseParser.UNION if all =>
        Union(left, right)
      case HoodieSqlBaseParser.UNION =>
        Distinct(Union(left, right))
      case HoodieSqlBaseParser.INTERSECT if all =>
        Intersect(left, right, isAll = true)
      case HoodieSqlBaseParser.INTERSECT =>
        Intersect(left, right, isAll = false)
      case HoodieSqlBaseParser.EXCEPT if all =>
        Except(left, right, isAll = true)
      case HoodieSqlBaseParser.EXCEPT =>
        Except(left, right, isAll = false)
      case HoodieSqlBaseParser.SETMINUS if all =>
        Except(left, right, isAll = true)
      case HoodieSqlBaseParser.SETMINUS =>
        Except(left, right, isAll = false)
    }
  }

  /**
    * Add an [[Aggregate]] or [[GroupingSets]] to a logical plan.
    */
  private def withAggregation(
                               ctx: AggregationContext,
                               selectExpressions: Seq[NamedExpression],
                               query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    val groupByExpressions = expressionList(ctx.groupingExpressions)

    if (ctx.GROUPING != null) {
      // GROUP BY .... GROUPING SETS (...)
      val selectedGroupByExprs =
        ctx.groupingSet.asScala.map(_.expression.asScala.map(e => expression(e)))
      GroupingSets(selectedGroupByExprs, groupByExpressions, query, selectExpressions)
    } else {
      // GROUP BY .... (WITH CUBE | WITH ROLLUP)?
      val mappedGroupByExpressions = if (ctx.CUBE != null) {
        Seq(Cube(groupByExpressions))
      } else if (ctx.ROLLUP != null) {
        Seq(Rollup(groupByExpressions))
      } else {
        groupByExpressions
      }
      Aggregate(mappedGroupByExpressions, selectExpressions, query)
    }
  }

  /**
    * Create a [[ScriptInputOutputSchema]].
    */
  protected def withScriptIOSchema(ctx: QuerySpecificationContext,
                                   inRowFormat: RowFormatContext,
                                   recordWriter: Token,
                                   outRowFormat: RowFormatContext,
                                   recordReader: Token,
                                   schemaLess: Boolean): ScriptInputOutputSchema = {
    if (recordWriter != null || recordReader != null) {
      // TODO: what does this message mean?
      throw new ParseException(
        "Unsupported operation: Used defined record reader/writer classes.", ctx)
    }

    // Decode and input/output format.
    type Format = (Seq[(String, String)], Option[String], Seq[(String, String)], Option[String])
    def format(
                fmt: RowFormatContext,
                configKey: String,
                defaultConfigValue: String): Format = fmt match {
      case c: RowFormatDelimitedContext =>
        // TODO we should use the visitRowFormatDelimited function here. However HiveScriptIOSchema
        // expects a seq of pairs in which the old parsers' token names are used as keys.
        // Transforming the result of visitRowFormatDelimited would be quite a bit messier than
        // retrieving the key value pairs ourselves.
        def entry(key: String, value: Token): Seq[(String, String)] = {
          Option(value).map(t => key -> t.getText).toSeq
        }
        val entries = entry("TOK_TABLEROWFORMATFIELD", c.fieldsTerminatedBy) ++
          entry("TOK_TABLEROWFORMATCOLLITEMS", c.collectionItemsTerminatedBy) ++
          entry("TOK_TABLEROWFORMATMAPKEYS", c.keysTerminatedBy) ++
          entry("TOK_TABLEROWFORMATLINES", c.linesSeparatedBy) ++
          entry("TOK_TABLEROWFORMATNULL", c.nullDefinedAs)

        (entries, None, Seq.empty, None)

      case c: RowFormatSerdeContext =>
        // Use a serde format.
        val CatalogStorageFormat(None, None, None, Some(name), _, props) = visitRowFormatSerde(c)

        // SPARK-10310: Special cases LazySimpleSerDe
        val recordHandler = if (name == "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe") {
          Option(conf.getConfString(configKey, defaultConfigValue))
        } else {
          None
        }
        (Seq.empty, Option(name), props.toSeq, recordHandler)

      case null =>
        // Use default (serde) format.
        val name = conf.getConfString("hive.script.serde",
          "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
        val props = Seq("field.delim" -> "\t")
        val recordHandler = Option(conf.getConfString(configKey, defaultConfigValue))
        (Nil, Option(name), props, recordHandler)
    }

    val (inFormat, inSerdeClass, inSerdeProps, reader) =
      format(
        inRowFormat, "hive.script.recordreader", "org.apache.hadoop.hive.ql.exec.TextRecordReader")

    val (outFormat, outSerdeClass, outSerdeProps, writer) =
      format(
        outRowFormat, "hive.script.recordwriter",
        "org.apache.hadoop.hive.ql.exec.TextRecordWriter")

    ScriptInputOutputSchema(
      inFormat, outFormat,
      inSerdeClass, outSerdeClass,
      inSerdeProps, outSerdeProps,
      reader, writer,
      schemaLess)
  }

  /**
    * Create a logical plan for a given 'FROM' clause. Note that we support multiple (comma
    * separated) relations here, these get converted into a single plan by condition-less inner join.
    */
  override def visitFromClause(ctx: FromClauseContext): LogicalPlan = withOrigin(ctx) {
    val from = ctx.relation.asScala.foldLeft(null: LogicalPlan) { (left, relation) =>
      val right = plan(relation.relationPrimary)
      val join = right.optionalMap(left)(Join(_, _, Inner, None))
      withJoinRelations(join, relation)
    }
    if (ctx.pivotClause() != null) {
      if (!ctx.lateralView.isEmpty) {
        throw new ParseException("LATERAL cannot be used together with PIVOT in FROM clause", ctx)
      }
      withPivot(ctx.pivotClause, from)
    } else {
      ctx.lateralView.asScala.foldLeft(from)(withGenerate)
    }
  }

  /**
    * Create a Pivot column value with or without an alias.
    */
  override def visitPivotValue(ctx: PivotValueContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.expression)
    if (ctx.identifier != null) {
      Alias(e, ctx.identifier.getText)()
    } else {
      e
    }
  }

  /**
    * Parse a qualified name to a multipart name.
    */
  override def visitQualifiedName(ctx: QualifiedNameContext): Seq[String] = withOrigin(ctx) {
    ctx.identifier.asScala.map(_.getText)
  }

  /**
    * Add a [[org.apache.spark.sql.catalyst.plans.logical.Generate]] (Lateral View) to a logical plan.
    */
  private def withGenerate(
                            query: LogicalPlan,
                            ctx: LateralViewContext): LogicalPlan = withOrigin(ctx) {
    val expressions = expressionList(ctx.expression)
    Generate(
      UnresolvedGenerator(visitFunctionName(ctx.qualifiedName), expressions),
      unrequiredChildIndex = Nil,
      outer = ctx.OUTER != null,
      Some(ctx.tblName.getText.toLowerCase(Locale.ROOT)),
      ctx.colName.asScala.map(_.getText).map(UnresolvedAttribute.apply),
      query)
  }

  /**
    * Create a single relation referenced in a FROM clause. This method is used when a part of the
    * join condition is nested, for example:
    * {{{
    *   select * from t1 join (t2 cross join t3) on col1 = col2
    * }}}
    */
  override def visitRelation(ctx: RelationContext): LogicalPlan = withOrigin(ctx) {
    withJoinRelations(plan(ctx.relationPrimary), ctx)
  }

  /**
    * Join one more [[LogicalPlan]]s to the current logical plan.
    */
  private def withJoinRelations(base: LogicalPlan, ctx: RelationContext): LogicalPlan = {
    ctx.joinRelation.asScala.foldLeft(base) { (left, join) =>
      withOrigin(join) {
        val baseJoinType = join.joinType match {
          case null => Inner
          case jt if jt.CROSS != null => Cross
          case jt if jt.FULL != null => FullOuter
          case jt if jt.SEMI != null => LeftSemi
          case jt if jt.ANTI != null => LeftAnti
          case jt if jt.LEFT != null => LeftOuter
          case jt if jt.RIGHT != null => RightOuter
          case _ => Inner
        }

        // Resolve the join type and join condition
        val (joinType, condition) = Option(join.joinCriteria) match {
          case Some(c) if c.USING != null =>
            (UsingJoin(baseJoinType, c.identifier.asScala.map(_.getText)), None)
          case Some(c) if c.booleanExpression != null =>
            (baseJoinType, Option(expression(c.booleanExpression)))
          case None if join.NATURAL != null =>
            if (baseJoinType == Cross) {
              throw new ParseException("NATURAL CROSS JOIN is not supported", ctx)
            }
            (NaturalJoin(baseJoinType), None)
          case None =>
            (baseJoinType, None)
        }
        Join(left, plan(join.right), joinType, condition)
      }
    }
  }

  /**
    * Add a [[org.apache.spark.sql.catalyst.plans.logical.Sample]] to a logical plan.
    *
    * This currently supports the following sampling methods:
    * - TABLESAMPLE(x ROWS): Sample the table down to the given number of rows.
    * - TABLESAMPLE(x PERCENT): Sample the table down to the given percentage. Note that percentages
    * are defined as a number between 0 and 100.
    * - TABLESAMPLE(BUCKET x OUT OF y): Sample the table down to a 'x' divided by 'y' fraction.
    */
  private def withSample(ctx: SampleContext, query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    // Create a sampled plan if we need one.
    def sample(fraction: Double): Sample = {
      // The range of fraction accepted by Sample is [0, 1]. Because Hive's block sampling
      // function takes X PERCENT as the input and the range of X is [0, 100], we need to
      // adjust the fraction.
      val eps = RandomSampler.roundingEpsilon
      validate(fraction >= 0.0 - eps && fraction <= 1.0 + eps,
        s"Sampling fraction ($fraction) must be on interval [0, 1]",
        ctx)
      Sample(0.0, fraction, withReplacement = false, (math.random * 1000).toInt, query)
    }

    if (ctx.sampleMethod() == null) {
      throw new ParseException("TABLESAMPLE does not accept empty inputs.", ctx)
    }

    ctx.sampleMethod() match {
      case ctx: SampleByRowsContext =>
        Limit(expression(ctx.expression), query)

      case ctx: SampleByPercentileContext =>
        val fraction = ctx.percentage.getText.toDouble
        val sign = if (ctx.negativeSign == null) 1 else -1
        sample(sign * fraction / 100.0d)

      case ctx: SampleByBytesContext =>
        val bytesStr = ctx.bytes.getText
        if (bytesStr.matches("[0-9]+[bBkKmMgG]")) {
          throw new ParseException("TABLESAMPLE(byteLengthLiteral) is not supported", ctx)
        } else {
          throw new ParseException(
            bytesStr + " is not a valid byte length literal, " +
              "expected syntax: DIGIT+ ('B' | 'K' | 'M' | 'G')", ctx)
        }

      case ctx: SampleByBucketContext if ctx.ON() != null =>
        if (ctx.identifier != null) {
          throw new ParseException(
            "TABLESAMPLE(BUCKET x OUT OF y ON colname) is not supported", ctx)
        } else {
          throw new ParseException(
            "TABLESAMPLE(BUCKET x OUT OF y ON function) is not supported", ctx)
        }

      case ctx: SampleByBucketContext =>
        sample(ctx.numerator.getText.toDouble / ctx.denominator.getText.toDouble)
    }
  }

  /**
    * Create a top-level plan with Common Table Expressions.
    */
  override def visitQuery(ctx: QueryContext): LogicalPlan = withOrigin(ctx) {
    val query = plan(ctx.queryNoWith)

    // Apply CTEs
    query.optional(ctx.ctes) {
      val ctes = ctx.ctes.namedQuery.asScala.map { nCtx =>
        val namedQuery = visitNamedQuery(nCtx)
        (namedQuery.alias, namedQuery)
      }
      // Check for duplicate names.
      checkDuplicateKeys(ctes, ctx)
      With(query, ctes)
    }
  }

  /**
    * Create a named logical plan.
    *
    * This is only used for Common Table Expressions.
    */
  override def visitNamedQuery(ctx: NamedQueryContext): SubqueryAlias = withOrigin(ctx) {
    SubqueryAlias(ctx.name.getText, plan(ctx.query))
  }

  /**
    * Create a logical plan for a sub-query.
    */
  override def visitSubquery(ctx: SubqueryContext): LogicalPlan = withOrigin(ctx) {
    plan(ctx.queryNoWith)
  }

  /**
    * Create an un-aliased table reference. This is typically used for top-level table references,
    * for example:
    * {{{
    *   INSERT INTO db.tbl2
    *   TABLE db.tbl1
    * }}}
    */
  override def visitTable(ctx: TableContext): LogicalPlan = withOrigin(ctx) {
    UnresolvedRelation(visitTableIdentifier(ctx.tableIdentifier))
  }

  /**
    * Create an aliased table reference. This is typically used in FROM clauses.
    */
  override def visitTableName(ctx: TableNameContext): LogicalPlan = withOrigin(ctx) {
    val tableId = visitTableIdentifier(ctx.tableIdentifier)
    val table = mayApplyAliasPlan(ctx.tableAlias, UnresolvedRelation(tableId))
    table.optionalMap(ctx.sample)(withSample)
  }

  /**
    * Create a table-valued function call with arguments, e.g. range(1000)
    */
  override def visitTableValuedFunction(ctx: TableValuedFunctionContext)
  : LogicalPlan = withOrigin(ctx) {
    val func = ctx.functionTable
    val aliases = if (func.tableAlias.identifierList != null) {
      visitIdentifierList(func.tableAlias.identifierList)
    } else {
      Seq.empty
    }

    val tvf = UnresolvedTableValuedFunction(
      func.identifier.getText, func.expression.asScala.map(expression), aliases)
    tvf.optionalMap(func.tableAlias.strictIdentifier)(aliasPlan)
  }

  /**
    * Create an inline table (a virtual table in Hive parlance).
    */
  override def visitInlineTable(ctx: InlineTableContext): LogicalPlan = withOrigin(ctx) {
    // Get the backing expressions.
    val rows = ctx.expression.asScala.map { e =>
      expression(e) match {
        // inline table comes in two styles:
        // style 1: values (1), (2), (3)  -- multiple columns are supported
        // style 2: values 1, 2, 3  -- only a single column is supported here
        case struct: CreateNamedStruct => struct.valExprs // style 1
        case child => Seq(child) // style 2
      }
    }

    val aliases = if (ctx.tableAlias.identifierList != null) {
      visitIdentifierList(ctx.tableAlias.identifierList)
    } else {
      Seq.tabulate(rows.head.size)(i => s"col${i + 1}")
    }

    val table = UnresolvedInlineTable(aliases, rows)
    table.optionalMap(ctx.tableAlias.strictIdentifier)(aliasPlan)
  }

  /**
    * Create an alias (SubqueryAlias) for a join relation. This is practically the same as
    * visitAliasedQuery and visitNamedExpression, ANTLR4 however requires us to use 3 different
    * hooks. We could add alias names for output columns, for example:
    * {{{
    *   SELECT a, b, c, d FROM (src1 s1 INNER JOIN src2 s2 ON s1.id = s2.id) dst(a, b, c, d)
    * }}}
    */
  override def visitAliasedRelation(ctx: AliasedRelationContext): LogicalPlan = withOrigin(ctx) {
    val relation = plan(ctx.relation).optionalMap(ctx.sample)(withSample)
    mayApplyAliasPlan(ctx.tableAlias, relation)
  }

  /**
    * Create an alias (SubqueryAlias) for a sub-query. This is practically the same as
    * visitAliasedRelation and visitNamedExpression, ANTLR4 however requires us to use 3 different
    * hooks. We could add alias names for output columns, for example:
    * {{{
    *   SELECT col1, col2 FROM testData AS t(col1, col2)
    * }}}
    */
  override def visitAliasedQuery(ctx: AliasedQueryContext): LogicalPlan = withOrigin(ctx) {
    val relation = plan(ctx.queryNoWith).optionalMap(ctx.sample)(withSample)
    if (ctx.tableAlias.strictIdentifier == null) {
      // For un-aliased subqueries, use a default alias name that is not likely to conflict with
      // normal subquery names, so that parent operators can only access the columns in subquery by
      // unqualified names. Users can still use this special qualifier to access columns if they
      // know it, but that's not recommended.
      SubqueryAlias("__auto_generated_subquery_name", relation)
    } else {
      mayApplyAliasPlan(ctx.tableAlias, relation)
    }
  }

  /**
    * Create an alias ([[SubqueryAlias]]) for a [[LogicalPlan]].
    */
  private def aliasPlan(alias: ParserRuleContext, plan: LogicalPlan): LogicalPlan = {
    SubqueryAlias(alias.getText, plan)
  }

  /**
    * If aliases specified in a FROM clause, create a subquery alias ([[SubqueryAlias]]) and
    * column aliases for a [[LogicalPlan]].
    */
  protected def mayApplyAliasPlan(tableAlias: TableAliasContext, plan: LogicalPlan): LogicalPlan = {
    if (tableAlias.strictIdentifier != null) {
      val subquery = SubqueryAlias(tableAlias.strictIdentifier.getText, plan)
      if (tableAlias.identifierList != null) {
        val columnNames = visitIdentifierList(tableAlias.identifierList)
        UnresolvedSubqueryColumnAliases(columnNames, subquery)
      } else {
        subquery
      }
    } else {
      plan
    }
  }

  /**
    * Create a Sequence of Strings for a parenthesis enclosed alias list.
    */
  override def visitIdentifierList(ctx: IdentifierListContext): Seq[String] = withOrigin(ctx) {
    visitIdentifierSeq(ctx.identifierSeq)
  }

  /**
    * Create a Sequence of Strings for an identifier list.
    */
  override def visitIdentifierSeq(ctx: IdentifierSeqContext): Seq[String] = withOrigin(ctx) {
    ctx.identifier.asScala.map(_.getText)
  }

  /* ********************************************************************************************
   * Table Identifier parsing
   * ******************************************************************************************** */
  /**
    * Create a [[TableIdentifier]] from a 'tableName' or 'databaseName'.'tableName' pattern.
    */
  override def visitTableIdentifier(
                                     ctx: TableIdentifierContext): TableIdentifier = withOrigin(ctx) {
    TableIdentifier(ctx.table.getText, Option(ctx.db).map(_.getText))
  }

  /* ********************************************************************************************
   * Expression parsing
   * ******************************************************************************************** */
  /**
    * Create an expression from the given context. This method just passes the context on to the
    * visitor and only takes care of typing (We assume that the visitor returns an Expression here).
    */
  protected def expression(ctx: ParserRuleContext): Expression = typedVisit(ctx)

  /**
    * Create sequence of expressions from the given sequence of contexts.
    */
  private def expressionList(trees: java.util.List[ExpressionContext]): Seq[Expression] = {
    trees.asScala.map(expression)
  }

  /**
    * Create a star (i.e. all) expression; this selects all elements (in the specified object).
    * Both un-targeted (global) and targeted aliases are supported.
    */
  override def visitStar(ctx: StarContext): Expression = withOrigin(ctx) {
    UnresolvedStar(Option(ctx.qualifiedName()).map(_.identifier.asScala.map(_.getText)))
  }

  /**
    * Create an aliased expression if an alias is specified. Both single and multi-aliases are
    * supported.
    */
  override def visitNamedExpression(ctx: NamedExpressionContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.expression)
    val expr = if (ctx.identifier != null) {
      Alias(e, ctx.identifier.getText)()
    } else if (ctx.identifierList != null) {
      MultiAlias(e, visitIdentifierList(ctx.identifierList))
    } else {
      e
    }
    expr
  }

  /**
    * Combine a number of boolean expressions into a balanced expression tree. These expressions are
    * either combined by a logical [[And]] or a logical [[Or]].
    *
    * A balanced binary tree is created because regular left recursive trees cause considerable
    * performance degradations and can cause stack overflows.
    */
  override def visitLogicalBinary(ctx: LogicalBinaryContext): Expression = withOrigin(ctx) {
    val expressionType = ctx.operator.getType
    val expressionCombiner = expressionType match {
      case AND => And.apply _
      case OR => Or.apply _
    }

    // Collect all similar left hand contexts.
    val contexts = ArrayBuffer(ctx.right)
    var current = ctx.left

    def collectContexts: Boolean = current match {
      case lbc: LogicalBinaryContext if lbc.operator.getType == expressionType =>
        contexts += lbc.right
        current = lbc.left
        true
      case _ =>
        contexts += current
        false
    }

    while (collectContexts) {
      // No body - all updates take place in the collectContexts.
    }

    // Reverse the contexts to have them in the same sequence as in the SQL statement & turn them
    // into expressions.
    val expressions = contexts.reverseMap(expression)

    // Create a balanced tree.
    def reduceToExpressionTree(low: Int, high: Int): Expression = high - low match {
      case 0 =>
        expressions(low)
      case 1 =>
        expressionCombiner(expressions(low), expressions(high))
      case x =>
        val mid = low + x / 2
        expressionCombiner(
          reduceToExpressionTree(low, mid),
          reduceToExpressionTree(mid + 1, high))
    }

    reduceToExpressionTree(0, expressions.size - 1)
  }

  /**
    * Invert a boolean expression.
    */
  override def visitLogicalNot(ctx: LogicalNotContext): Expression = withOrigin(ctx) {
    Not(expression(ctx.booleanExpression()))
  }

  /**
    * Create a filtering correlated sub-query (EXISTS).
    */
  override def visitExists(ctx: ExistsContext): Expression = {
    Exists(plan(ctx.query))
  }

  /**
    * Create a comparison expression. This compares two expressions. The following comparison
    * operators are supported:
    * - Equal: '=' or '=='
    * - Null-safe Equal: '<=>'
    * - Not Equal: '<>' or '!='
    * - Less than: '<'
    * - Less then or Equal: '<='
    * - Greater than: '>'
    * - Greater then or Equal: '>='
    */
  override def visitComparison(ctx: ComparisonContext): Expression = withOrigin(ctx) {
    val left = expression(ctx.left)
    val right = expression(ctx.right)
    val operator = ctx.comparisonOperator().getChild(0).asInstanceOf[TerminalNode]
    operator.getSymbol.getType match {
      case EQ =>
        EqualTo(left, right)
      case NSEQ =>
        EqualNullSafe(left, right)
      case NEQ | NEQJ =>
        Not(EqualTo(left, right))
      case LT =>
        LessThan(left, right)
      case LTE =>
        LessThanOrEqual(left, right)
      case GT =>
        GreaterThan(left, right)
      case GTE =>
        GreaterThanOrEqual(left, right)
    }
  }

  /**
    * Create a predicated expression. A predicated expression is a normal expression with a
    * predicate attached to it, for example:
    * {{{
    *    a + 1 IS NULL
    * }}}
    */
  override def visitPredicated(ctx: PredicatedContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.valueExpression)
    if (ctx.predicate != null) {
      withPredicate(e, ctx.predicate)
    } else {
      e
    }
  }

  /**
    * Add a predicate to the given expression. Supported expressions are:
    * - (NOT) BETWEEN
    * - (NOT) IN
    * - (NOT) LIKE
    * - (NOT) RLIKE
    * - IS (NOT) NULL.
    * - IS (NOT) DISTINCT FROM
    */
  private def withPredicate(e: Expression, ctx: PredicateContext): Expression = withOrigin(ctx) {
    // Invert a predicate if it has a valid NOT clause.
    def invertIfNotDefined(e: Expression): Expression = ctx.NOT match {
      case null => e
      case not => Not(e)
    }

    def getValueExpressions(e: Expression): Seq[Expression] = e match {
      case c: CreateNamedStruct => c.valExprs
      case other => Seq(other)
    }

    // Create the predicate.
    ctx.kind.getType match {
      case BETWEEN =>
        // BETWEEN is translated to lower <= e && e <= upper
        invertIfNotDefined(And(
          GreaterThanOrEqual(e, expression(ctx.lower)),
          LessThanOrEqual(e, expression(ctx.upper))))
      case IN if ctx.query != null =>
        invertIfNotDefined(InSubquery(getValueExpressions(e), ListQuery(plan(ctx.query))))
      case IN =>
        invertIfNotDefined(In(e, ctx.expression.asScala.map(expression)))
      case LIKE =>
        invertIfNotDefined(Like(e, expression(ctx.pattern)))
      case RLIKE =>
        invertIfNotDefined(RLike(e, expression(ctx.pattern)))
      case NULL if ctx.NOT != null =>
        IsNotNull(e)
      case NULL =>
        IsNull(e)
      case DISTINCT if ctx.NOT != null =>
        EqualNullSafe(e, expression(ctx.right))
      case DISTINCT =>
        Not(EqualNullSafe(e, expression(ctx.right)))
    }
  }

  /**
    * Create a binary arithmetic expression. The following arithmetic operators are supported:
    * - Multiplication: '*'
    * - Division: '/'
    * - Hive Long Division: 'DIV'
    * - Modulo: '%'
    * - Addition: '+'
    * - Subtraction: '-'
    * - Binary AND: '&'
    * - Binary XOR
    * - Binary OR: '|'
    */
  override def visitArithmeticBinary(ctx: ArithmeticBinaryContext): Expression = withOrigin(ctx) {
    val left = expression(ctx.left)
    val right = expression(ctx.right)
    ctx.operator.getType match {
      case ASTERISK =>
        Multiply(left, right)
      case SLASH =>
        Divide(left, right)
      case PERCENT =>
        Remainder(left, right)
      case DIV =>
        Cast(Divide(left, right), LongType)
      case PLUS =>
        Add(left, right)
      case MINUS =>
        Subtract(left, right)
      case CONCAT_PIPE =>
        Concat(left :: right :: Nil)
      case AMPERSAND =>
        BitwiseAnd(left, right)
      case HAT =>
        BitwiseXor(left, right)
      case PIPE =>
        BitwiseOr(left, right)
    }
  }

  /**
    * Create a unary arithmetic expression. The following arithmetic operators are supported:
    * - Plus: '+'
    * - Minus: '-'
    * - Bitwise Not: '~'
    */
  override def visitArithmeticUnary(ctx: ArithmeticUnaryContext): Expression = withOrigin(ctx) {
    val value = expression(ctx.valueExpression)
    ctx.operator.getType match {
      case PLUS =>
        value
      case MINUS =>
        UnaryMinus(value)
      case TILDE =>
        BitwiseNot(value)
    }
  }

  /**
    * Create a [[Cast]] expression.
    */
  override def visitCast(ctx: CastContext): Expression = withOrigin(ctx) {
    Cast(expression(ctx.expression), visitSparkDataType(ctx.dataType))
  }

  /**
    * Create a [[CreateStruct]] expression.
    */
  override def visitStruct(ctx: StructContext): Expression = withOrigin(ctx) {
    CreateStruct(ctx.argument.asScala.map(expression))
  }

  /**
    * Create a [[First]] expression.
    */
  override def visitFirst(ctx: FirstContext): Expression = withOrigin(ctx) {
    val ignoreNullsExpr = ctx.IGNORE != null
    First(expression(ctx.expression), Literal(ignoreNullsExpr)).toAggregateExpression()
  }

  /**
    * Create a [[Last]] expression.
    */
  override def visitLast(ctx: LastContext): Expression = withOrigin(ctx) {
    val ignoreNullsExpr = ctx.IGNORE != null
    Last(expression(ctx.expression), Literal(ignoreNullsExpr)).toAggregateExpression()
  }

  /**
    * Create a Position expression.
    */
  override def visitPosition(ctx: PositionContext): Expression = withOrigin(ctx) {
    new StringLocate(expression(ctx.substr), expression(ctx.str))
  }

  /**
    * Create a Extract expression.
    */
  override def visitExtract(ctx: ExtractContext): Expression = withOrigin(ctx) {
    ctx.field.getText.toUpperCase(Locale.ROOT) match {
      case "YEAR" =>
        Year(expression(ctx.source))
      case "QUARTER" =>
        Quarter(expression(ctx.source))
      case "MONTH" =>
        Month(expression(ctx.source))
      case "WEEK" =>
        WeekOfYear(expression(ctx.source))
      case "DAY" =>
        DayOfMonth(expression(ctx.source))
      case "DAYOFWEEK" =>
        DayOfWeek(expression(ctx.source))
      case "HOUR" =>
        Hour(expression(ctx.source))
      case "MINUTE" =>
        Minute(expression(ctx.source))
      case "SECOND" =>
        Second(expression(ctx.source))
      case other =>
        throw new ParseException(s"Literals of type '$other' are currently not supported.", ctx)
    }
  }

  /**
    * Create a (windowed) Function expression.
    */
  override def visitFunctionCall(ctx: FunctionCallContext): Expression = withOrigin(ctx) {
    def replaceFunctions(
                          funcID: FunctionIdentifier,
                          ctx: FunctionCallContext): FunctionIdentifier = {
      val opt = ctx.trimOption
      if (opt != null) {
        if (ctx.qualifiedName.getText.toLowerCase(Locale.ROOT) != "trim") {
          throw new ParseException(s"The specified function ${ctx.qualifiedName.getText} " +
            s"doesn't support with option ${opt.getText}.", ctx)
        }
        opt.getType match {
          case BOTH => funcID
          case LEADING => funcID.copy(funcName = "ltrim")
          case TRAILING => funcID.copy(funcName = "rtrim")
          case _ => throw new ParseException("Function trim doesn't support with " +
            s"type ${opt.getType}. Please use BOTH, LEADING or Trailing as trim type", ctx)
        }
      } else {
        funcID
      }
    }

    // Create the function call.
    val name = ctx.qualifiedName.getText
    val isDistinct = Option(ctx.setQuantifier()).exists(_.DISTINCT != null)
    val arguments = ctx.argument.asScala.map(expression) match {
      case Seq(UnresolvedStar(None))
        if name.toLowerCase(Locale.ROOT) == "count" && !isDistinct =>
        // Transform COUNT(*) into COUNT(1).
        Seq(Literal(1))
      case expressions =>
        expressions
    }
    val funcId = replaceFunctions(visitFunctionName(ctx.qualifiedName), ctx)
    val function = UnresolvedFunction(funcId, arguments, isDistinct)


    // Check if the function is evaluated in a windowed context.
    ctx.windowSpec match {
      case spec: WindowRefContext =>
        UnresolvedWindowExpression(function, visitWindowRef(spec))
      case spec: WindowDefContext =>
        WindowExpression(function, visitWindowDef(spec))
      case _ => function
    }
  }

  /**
    * Create a function database (optional) and name pair.
    */
  protected def visitFunctionName(ctx: QualifiedNameContext): FunctionIdentifier = {
    ctx.identifier().asScala.map(_.getText) match {
      case Seq(db, fn) => FunctionIdentifier(fn, Option(db))
      case Seq(fn) => FunctionIdentifier(fn, None)
      case other => throw new ParseException(s"Unsupported function name '${ctx.getText}'", ctx)
    }
  }

  /**
    * Create an [[LambdaFunction]].
    */
  override def visitLambda(ctx: LambdaContext): Expression = withOrigin(ctx) {
    val arguments = ctx.IDENTIFIER().asScala.map { name =>
      UnresolvedNamedLambdaVariable(UnresolvedAttribute.quoted(name.getText).nameParts)
    }
    val function = expression(ctx.expression).transformUp {
      case a: UnresolvedAttribute => UnresolvedNamedLambdaVariable(a.nameParts)
    }
    LambdaFunction(function, arguments)
  }

  /**
    * Create a reference to a window frame, i.e. [[WindowSpecReference]].
    */
  override def visitWindowRef(ctx: WindowRefContext): WindowSpecReference = withOrigin(ctx) {
    WindowSpecReference(ctx.identifier.getText)
  }

  /**
    * Create a window definition, i.e. [[WindowSpecDefinition]].
    */
  override def visitWindowDef(ctx: WindowDefContext): WindowSpecDefinition = withOrigin(ctx) {
    // CLUSTER BY ... | PARTITION BY ... ORDER BY ...
    val partition = ctx.partition.asScala.map(expression)
    val order = ctx.sortItem.asScala.map(visitSortItem)

    // RANGE/ROWS BETWEEN ...
    val frameSpecOption = Option(ctx.windowFrame).map { frame =>
      val frameType = frame.frameType.getType match {
        case RANGE => RangeFrame
        case ROWS => RowFrame
      }

      SpecifiedWindowFrame(
        frameType,
        visitFrameBound(frame.start),
        Option(frame.end).map(visitFrameBound).getOrElse(CurrentRow))
    }

    WindowSpecDefinition(
      partition,
      order,
      frameSpecOption.getOrElse(UnspecifiedFrame))
  }

  /**
    * Create or resolve a frame boundary expressions.
    */
  override def visitFrameBound(ctx: FrameBoundContext): Expression = withOrigin(ctx) {
    def value: Expression = {
      val e = expression(ctx.expression)
      validate(e.resolved && e.foldable, "Frame bound value must be a literal.", ctx)
      e
    }

    ctx.boundType.getType match {
      case PRECEDING if ctx.UNBOUNDED != null =>
        UnboundedPreceding
      case PRECEDING =>
        UnaryMinus(value)
      case CURRENT =>
        CurrentRow
      case FOLLOWING if ctx.UNBOUNDED != null =>
        UnboundedFollowing
      case FOLLOWING =>
        value
    }
  }

  /**
    * Create a [[CreateStruct]] expression.
    */
  override def visitRowConstructor(ctx: RowConstructorContext): Expression = withOrigin(ctx) {
    CreateStruct(ctx.namedExpression().asScala.map(expression))
  }

  /**
    * Create a [[ScalarSubquery]] expression.
    */
  override def visitSubqueryExpression(
                                        ctx: SubqueryExpressionContext): Expression = withOrigin(ctx) {
    ScalarSubquery(plan(ctx.query))
  }

  /**
    * Create a value based [[CaseWhen]] expression. This has the following SQL form:
    * {{{
    *   CASE [expression]
    *    WHEN [value] THEN [expression]
    *    ...
    *    ELSE [expression]
    *   END
    * }}}
    */
  override def visitSimpleCase(ctx: SimpleCaseContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.value)
    val branches = ctx.whenClause.asScala.map { wCtx =>
      (EqualTo(e, expression(wCtx.condition)), expression(wCtx.result))
    }
    CaseWhen(branches, Option(ctx.elseExpression).map(expression))
  }

  /**
    * Create a condition based [[CaseWhen]] expression. This has the following SQL syntax:
    * {{{
    *   CASE
    *    WHEN [predicate] THEN [expression]
    *    ...
    *    ELSE [expression]
    *   END
    * }}}
    *
    * @param ctx the parse tree
    */
  override def visitSearchedCase(ctx: SearchedCaseContext): Expression = withOrigin(ctx) {
    val branches = ctx.whenClause.asScala.map { wCtx =>
      (expression(wCtx.condition), expression(wCtx.result))
    }
    CaseWhen(branches, Option(ctx.elseExpression).map(expression))
  }

  /**
    * Currently only regex in expressions of SELECT statements are supported; in other
    * places, e.g., where `(a)?+.+` = 2, regex are not meaningful.
    */
  private def canApplyRegex(ctx: ParserRuleContext): Boolean = withOrigin(ctx) {
    var parent = ctx.getParent
    while (parent != null) {
      if (parent.isInstanceOf[NamedExpressionContext]) return true
      parent = parent.getParent
    }
    return false
  }

  /**
    * Create a dereference expression. The return type depends on the type of the parent.
    * If the parent is an [[UnresolvedAttribute]], it can be a [[UnresolvedAttribute]] or
    * a [[UnresolvedRegex]] for regex quoted in ``; if the parent is some other expression,
    * it can be [[UnresolvedExtractValue]].
    */
  override def visitDereference(ctx: DereferenceContext): Expression = withOrigin(ctx) {
    val attr = ctx.fieldName.getText
    expression(ctx.base) match {
      case unresolved_attr@UnresolvedAttribute(nameParts) =>
        ctx.fieldName.getStart.getText match {
          case escapedIdentifier(columnNameRegex)
            if conf.supportQuotedRegexColumnName && canApplyRegex(ctx) =>
            UnresolvedRegex(columnNameRegex, Some(unresolved_attr.name),
              conf.caseSensitiveAnalysis)
          case _ =>
            UnresolvedAttribute(nameParts :+ attr)
        }
      case e =>
        UnresolvedExtractValue(e, Literal(attr))
    }
  }

  /**
    * Create an [[UnresolvedAttribute]] expression or a [[UnresolvedRegex]] if it is a regex
    * quoted in ``
    */
  override def visitColumnReference(ctx: ColumnReferenceContext): Expression = withOrigin(ctx) {
    ctx.getStart.getText match {
      case escapedIdentifier(columnNameRegex)
        if conf.supportQuotedRegexColumnName && canApplyRegex(ctx) =>
        UnresolvedRegex(columnNameRegex, None, conf.caseSensitiveAnalysis)
      case _ =>
        UnresolvedAttribute.quoted(ctx.getText)
    }

  }

  /**
    * Create an [[UnresolvedExtractValue]] expression, this is used for subscript access to an array.
    */
  override def visitSubscript(ctx: SubscriptContext): Expression = withOrigin(ctx) {
    UnresolvedExtractValue(expression(ctx.value), expression(ctx.index))
  }

  /**
    * Create an expression for an expression between parentheses. This is need because the ANTLR
    * visitor cannot automatically convert the nested context into an expression.
    */
  override def visitParenthesizedExpression(
                                             ctx: ParenthesizedExpressionContext): Expression = withOrigin(ctx) {
    expression(ctx.expression)
  }

  /**
    * Create a [[SortOrder]] expression.
    */
  override def visitSortItem(ctx: SortItemContext): SortOrder = withOrigin(ctx) {
    val direction = if (ctx.DESC != null) {
      Descending
    } else {
      Ascending
    }
    val nullOrdering = if (ctx.FIRST != null) {
      NullsFirst
    } else if (ctx.LAST != null) {
      NullsLast
    } else {
      direction.defaultNullOrdering
    }
    SortOrder(expression(ctx.expression), direction, nullOrdering, Set.empty)
  }

  /**
    * Create a typed Literal expression. A typed literal has the following SQL syntax:
    * {{{
    *   [TYPE] '[VALUE]'
    * }}}
    * Currently Date, Timestamp and Binary typed literals are supported.
    */
  override def visitTypeConstructor(ctx: TypeConstructorContext): Literal = withOrigin(ctx) {
    val value = string(ctx.STRING)
    val valueType = ctx.identifier.getText.toUpperCase(Locale.ROOT)
    try {
      valueType match {
        case "DATE" =>
          Literal(Date.valueOf(value))
        case "TIMESTAMP" =>
          Literal(Timestamp.valueOf(value))
        case "X" =>
          val padding = if (value.length % 2 != 0) "0" else ""
          Literal(DatatypeConverter.parseHexBinary(padding + value))
        case other =>
          throw new ParseException(s"Literals of type '$other' are currently not supported.", ctx)
      }
    } catch {
      case e: IllegalArgumentException =>
        val message = Option(e.getMessage).getOrElse(s"Exception parsing $valueType")
        throw new ParseException(message, ctx)
    }
  }

  /**
    * Create a NULL literal expression.
    */
  override def visitNullLiteral(ctx: NullLiteralContext): Literal = withOrigin(ctx) {
    Literal(null)
  }

  /**
    * Create a Boolean literal expression.
    */
  override def visitBooleanLiteral(ctx: BooleanLiteralContext): Literal = withOrigin(ctx) {
    if (ctx.getText.toBoolean) {
      Literal.TrueLiteral
    } else {
      Literal.FalseLiteral
    }
  }

  /**
    * Create an integral literal expression. The code selects the most narrow integral type
    * possible, either a BigDecimal, a Long or an Integer is returned.
    */
  override def visitIntegerLiteral(ctx: IntegerLiteralContext): Literal = withOrigin(ctx) {
    BigDecimal(ctx.getText) match {
      case v if v.isValidInt =>
        Literal(v.intValue())
      case v if v.isValidLong =>
        Literal(v.longValue())
      case v => Literal(v.underlying())
    }
  }

  /**
    * Create a decimal literal for a regular decimal number.
    */
  override def visitDecimalLiteral(ctx: DecimalLiteralContext): Literal = withOrigin(ctx) {
    Literal(BigDecimal(ctx.getText).underlying())
  }

  /** Create a numeric literal expression. */
  private def numericLiteral
  (ctx: NumberContext, minValue: BigDecimal, maxValue: BigDecimal, typeName: String)
  (converter: String => Any): Literal = withOrigin(ctx) {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    try {
      val rawBigDecimal = BigDecimal(rawStrippedQualifier)
      if (rawBigDecimal < minValue || rawBigDecimal > maxValue) {
        throw new ParseException(s"Numeric literal ${rawStrippedQualifier} does not " +
          s"fit in range [${minValue}, ${maxValue}] for type ${typeName}", ctx)
      }
      Literal(converter(rawStrippedQualifier))
    } catch {
      case e: NumberFormatException =>
        throw new ParseException(e.getMessage, ctx)
    }
  }

  /**
    * Create a Byte Literal expression.
    */
  override def visitTinyIntLiteral(ctx: TinyIntLiteralContext): Literal = {
    numericLiteral(ctx, Byte.MinValue, Byte.MaxValue, ByteType.simpleString)(_.toByte)
  }

  /**
    * Create a Short Literal expression.
    */
  override def visitSmallIntLiteral(ctx: SmallIntLiteralContext): Literal = {
    numericLiteral(ctx, Short.MinValue, Short.MaxValue, ShortType.simpleString)(_.toShort)
  }

  /**
    * Create a Long Literal expression.
    */
  override def visitBigIntLiteral(ctx: BigIntLiteralContext): Literal = {
    numericLiteral(ctx, Long.MinValue, Long.MaxValue, LongType.simpleString)(_.toLong)
  }

  /**
    * Create a Double Literal expression.
    */
  override def visitDoubleLiteral(ctx: DoubleLiteralContext): Literal = {
    numericLiteral(ctx, Double.MinValue, Double.MaxValue, DoubleType.simpleString)(_.toDouble)
  }

  /**
    * Create a BigDecimal Literal expression.
    */
  override def visitBigDecimalLiteral(ctx: BigDecimalLiteralContext): Literal = {
    val raw = ctx.getText.substring(0, ctx.getText.length - 2)
    try {
      Literal(BigDecimal(raw).underlying())
    } catch {
      case e: AnalysisException =>
        throw new ParseException(e.message, ctx)
    }
  }

  /**
    * Create a String literal expression.
    */
  override def visitStringLiteral(ctx: StringLiteralContext): Literal = withOrigin(ctx) {
    Literal(createString(ctx))
  }

  /**
    * Create a String from a string literal context. This supports multiple consecutive string
    * literals, these are concatenated, for example this expression "'hello' 'world'" will be
    * converted into "helloworld".
    *
    * Special characters can be escaped by using Hive/C-style escaping.
    */
  private def createString(ctx: StringLiteralContext): String = {
    if (conf.escapedStringLiterals) {
      ctx.STRING().asScala.map(stringWithoutUnescape).mkString
    } else {
      ctx.STRING().asScala.map(string).mkString
    }
  }

  /**
    * Create a [[CalendarInterval]] literal expression. An interval expression can contain multiple
    * unit value pairs, for instance: interval 2 months 2 days.
    */
  override def visitInterval(ctx: IntervalContext): Literal = withOrigin(ctx) {
    val intervals = ctx.intervalField.asScala.map(visitIntervalField)
    validate(intervals.nonEmpty, "at least one time unit should be given for interval literal", ctx)
    Literal(intervals.reduce(_.add(_)))
  }

  /**
    * Create a [[CalendarInterval]] for a unit value pair. Two unit configuration types are
    * supported:
    * - Single unit.
    * - From-To unit (only 'YEAR TO MONTH' and 'DAY TO SECOND' are supported).
    */
  override def visitIntervalField(ctx: IntervalFieldContext): CalendarInterval = withOrigin(ctx) {
    import ctx._
    val s = value.getText
    try {
      val unitText = unit.getText.toLowerCase(Locale.ROOT)
      val interval = (unitText, Option(to).map(_.getText.toLowerCase(Locale.ROOT))) match {
        case (u, None) if u.endsWith("s") =>
          // Handle plural forms, e.g: yearS/monthS/weekS/dayS/hourS/minuteS/hourS/...
          CalendarInterval.fromSingleUnitString(u.substring(0, u.length - 1), s)
        case (u, None) =>
          CalendarInterval.fromSingleUnitString(u, s)
        case ("year", Some("month")) =>
          CalendarInterval.fromYearMonthString(s)
        case ("day", Some("second")) =>
          CalendarInterval.fromDayTimeString(s)
        case (from, Some(t)) =>
          throw new ParseException(s"Intervals FROM $from TO $t are not supported.", ctx)
      }
      validate(interval != null, "No interval can be constructed", ctx)
      interval
    } catch {
      // Handle Exceptions thrown by CalendarInterval
      case e: IllegalArgumentException =>
        val pe = new ParseException(e.getMessage, ctx)
        pe.setStackTrace(e.getStackTrace)
        throw pe
    }
  }

  /* ********************************************************************************************
   * DataType parsing
   * ******************************************************************************************** */
  /**
    * Create a Spark DataType.
    */
  private def visitSparkDataType(ctx: DataTypeContext): DataType = {
    HiveStringType.replaceCharType(typedVisit(ctx))
  }

  /**
    * Resolve/create a primitive type.
    */
  override def visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): DataType = withOrigin(ctx) {
    val dataType = ctx.identifier.getText.toLowerCase(Locale.ROOT)
    (dataType, ctx.INTEGER_VALUE().asScala.toList) match {
      case ("boolean", Nil) => BooleanType
      case ("tinyint" | "byte", Nil) => ByteType
      case ("smallint" | "short", Nil) => ShortType
      case ("int" | "integer", Nil) => IntegerType
      case ("bigint" | "long", Nil) => LongType
      case ("float", Nil) => FloatType
      case ("double", Nil) => DoubleType
      case ("date", Nil) => DateType
      case ("timestamp", Nil) => TimestampType
      case ("string", Nil) => StringType
      case ("char", length :: Nil) => CharType(length.getText.toInt)
      case ("varchar", length :: Nil) => VarcharType(length.getText.toInt)
      case ("binary", Nil) => BinaryType
      case ("decimal", Nil) => DecimalType.USER_DEFAULT
      case ("decimal", precision :: Nil) => DecimalType(precision.getText.toInt, 0)
      case ("decimal", precision :: scale :: Nil) =>
        DecimalType(precision.getText.toInt, scale.getText.toInt)
      case (dt, params) =>
        val dtStr = if (params.nonEmpty) s"$dt(${params.mkString(",")})" else dt
        throw new ParseException(s"DataType $dtStr is not supported.", ctx)
    }
  }

  /**
    * Create a complex DataType. Arrays, Maps and Structures are supported.
    */
  override def visitComplexDataType(ctx: ComplexDataTypeContext): DataType = withOrigin(ctx) {
    ctx.complex.getType match {
      case ARRAY =>
        ArrayType(typedVisit(ctx.dataType(0)))
      case MAP =>
        MapType(typedVisit(ctx.dataType(0)), typedVisit(ctx.dataType(1)))
      case STRUCT =>
        StructType(Option(ctx.complexColTypeList).toSeq.flatMap(visitComplexColTypeList))
    }
  }

  /**
    * Create top level table schema.
    */
  protected def createSchema(ctx: ColTypeListContext): StructType = {
    StructType(Option(ctx).toSeq.flatMap(visitColTypeList))
  }

  /**
    * Create a [[StructType]] from a number of column definitions.
    */
  override def visitColTypeList(ctx: ColTypeListContext): Seq[StructField] = withOrigin(ctx) {
    ctx.colType().asScala.map(visitColType)
  }

  /**
    * Create a top level [[StructField]] from a column definition.
    */
  override def visitColType(ctx: ColTypeContext): StructField = withOrigin(ctx) {
    import ctx._

    val builder = new MetadataBuilder
    // Add comment to metadata
    if (STRING != null) {
      builder.putString("comment", string(STRING))
    }
    // Add Hive type string to metadata.
    val rawDataType = typedVisit[DataType](ctx.dataType)
    val cleanedDataType = HiveStringType.replaceCharType(rawDataType)
    if (rawDataType != cleanedDataType) {
      builder.putString(HIVE_TYPE_STRING, rawDataType.catalogString)
    }

    StructField(
      identifier.getText,
      cleanedDataType,
      nullable = true,
      builder.build())
  }

  /**
    * Create a [[StructType]] from a sequence of [[StructField]]s.
    */
  protected def createStructType(ctx: ComplexColTypeListContext): StructType = {
    StructType(Option(ctx).toSeq.flatMap(visitComplexColTypeList))
  }

  /**
    * Create a [[StructType]] from a number of column definitions.
    */
  override def visitComplexColTypeList(
                                        ctx: ComplexColTypeListContext): Seq[StructField] = withOrigin(ctx) {
    ctx.complexColType().asScala.map(visitComplexColType)
  }

  /**
    * Create a [[StructField]] from a column definition.
    */
  override def visitComplexColType(ctx: ComplexColTypeContext): StructField = withOrigin(ctx) {
    import ctx._
    val structField = StructField(identifier.getText, typedVisit(dataType), nullable = true)
    if (STRING == null) structField else structField.withComment(string(STRING))
  }
}

