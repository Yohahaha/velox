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
package io.glutenproject.execution

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.extension.ValidationResult
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.sql.shims.SparkShimLoader
import io.glutenproject.substrait.{JoinParams, SubstraitContext}
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.protobuf.StringValue
import io.substrait.proto.JoinRel

import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConverters._

/** Performs a sort merge join of two child relations. */
case class SortMergeJoinExecTransformer(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean = false,
    projectList: Seq[NamedExpression] = null)
  extends BinaryExecNode
  with TransformSupport {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genSortMergeJoinTransformerMetrics(sparkContext)

  val (bufferedKeys, streamedKeys, bufferedPlan, streamedPlan) =
    (rightKeys, leftKeys, right, left)

  override def stringArgs: Iterator[Any] = super.stringArgs.toSeq.dropRight(1).iterator

  override def simpleStringWithNodeId(): String = {
    val opId = ExplainUtils.getOpId(this)
    s"$nodeName $joinType ($opId)".trim
  }

  override def nodeName: String = {
    if (isSkewJoin) super.nodeName + "(skew=true)" else super.nodeName
  }

  override def verboseStringWithOperatorId(): String = {
    val joinCondStr = if (condition.isDefined) {
      s"${condition.get}"
    } else "None"
    s"""
       |(${ExplainUtils.getOpId(this)}) $nodeName
       |${ExplainUtils.generateFieldString("Left keys", leftKeys)}
       |${ExplainUtils.generateFieldString("Right keys", rightKeys)}
       |${ExplainUtils.generateFieldString("Join condition", joinCondStr)}
    """.stripMargin
  }

  override def output: Seq[Attribute] = {
    if (projectList != null && projectList.nonEmpty) {
      return projectList.map(_.toAttribute)
    }
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        (left.output ++ right.output).map(_.withNullability(true))
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"${getClass.getSimpleName} should not take $x as the JoinType")
    }
  }

  override def outputPartitioning: Partitioning = joinType match {
    case _: InnerLike =>
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    // For left and right outer joins, the output is partitioned by the streamed input's join keys.
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case LeftExistence(_) => left.outputPartitioning
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    if (isSkewJoin) {
      // We re-arrange the shuffle partitions to deal with skew join, and the new children
      // partitioning doesn't satisfy `HashClusteredDistribution`.
      UnspecifiedDistribution :: UnspecifiedDistribution :: Nil
    } else {
      SparkShimLoader.getSparkShims.getDistribution(leftKeys, rightKeys)
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  override def outputOrdering: Seq[SortOrder] = joinType match {
    // For inner join, orders of both sides keys should be kept.
    case _: InnerLike =>
      val leftKeyOrdering = getKeyOrdering(leftKeys, left.outputOrdering)
      val rightKeyOrdering = getKeyOrdering(rightKeys, right.outputOrdering)
      leftKeyOrdering.zip(rightKeyOrdering).map {
        case (lKey, rKey) =>
          // Also add the right key and its `sameOrderExpressions`
          val sameOrderExpressions = ExpressionSet(lKey.sameOrderExpressions ++ rKey.children)
          SortOrder(lKey.child, Ascending, sameOrderExpressions.toSeq)
      }
    // For left and right outer joins, the output is ordered by the streamed input's join keys.
    case LeftOuter => getKeyOrdering(leftKeys, left.outputOrdering)
    case RightOuter => getKeyOrdering(rightKeys, right.outputOrdering)
    // There are null rows in both streams, so there is no order.
    case FullOuter => Nil
    case LeftExistence(_) => getKeyOrdering(leftKeys, left.outputOrdering)
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  private def getKeyOrdering(
      keys: Seq[Expression],
      childOutputOrdering: Seq[SortOrder]): Seq[SortOrder] = {
    val requiredOrdering = requiredOrders(keys)
    if (SortOrder.orderingSatisfies(childOutputOrdering, requiredOrdering)) {
      keys.zip(childOutputOrdering).map {
        case (key, childOrder) =>
          val sameOrderExpressionsSet = ExpressionSet(childOrder.children) - key
          SortOrder(key, Ascending, sameOrderExpressionsSet.toSeq)
      }
    } else {
      requiredOrdering
    }
  }

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    keys.map(SortOrder(_, Ascending))
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    getColumnarInputRDDs(streamedPlan) ++ getColumnarInputRDDs(bufferedPlan)
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genSortMergeJoinTransformerMetricsUpdater(metrics)

  def genJoinParametersBuilder(): com.google.protobuf.Any.Builder = {
    val (isSMJ, isNullAwareAntiJoin) = (1, 0)
    // Start with "JoinParameters:"
    val joinParametersStr = new StringBuffer("JoinParameters:")
    // isSMJ: 0 for SHJ, 1 for SMJ
    // isNullAwareAntiJoin: 0 for false, 1 for true
    joinParametersStr
      .append("isSMJ=")
      .append(isSMJ)
      .append("\n")
      .append("isNullAwareAntiJoin=")
      .append(isNullAwareAntiJoin)
      .append("\n")
      .append("isExistenceJoin=")
      .append(if (joinType.isInstanceOf[ExistenceJoin]) 1 else 0)
      .append("\n")
    val message = StringValue
      .newBuilder()
      .setValue(joinParametersStr.toString)
      .build()
    com.google.protobuf.Any.newBuilder
      .setValue(message.toByteString)
      .setTypeUrl("/google.protobuf.StringValue")
  }

  // Direct output order of substrait join operation
  protected val substraitJoinType = joinType match {
    case Inner =>
      JoinRel.JoinType.JOIN_TYPE_INNER
    case FullOuter =>
      JoinRel.JoinType.JOIN_TYPE_OUTER
    case LeftOuter =>
      JoinRel.JoinType.JOIN_TYPE_LEFT
    case RightOuter =>
      JoinRel.JoinType.JOIN_TYPE_RIGHT
    case LeftSemi =>
      JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
    case LeftAnti =>
      JoinRel.JoinType.JOIN_TYPE_ANTI
    case _ =>
      // TODO: Support cross join with Cross Rel
      // TODO: Support existence join
      JoinRel.JoinType.UNRECOGNIZED
  }

  override protected def doValidateInternal(): ValidationResult = {
    val substraitContext = new SubstraitContext
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    if (substraitJoinType == JoinRel.JoinType.UNRECOGNIZED) {
      return ValidationResult
        .notOk(s"Found unsupported join type of $joinType for substrait: $substraitJoinType")
    }
    val relNode = JoinUtils.createJoinRel(
      streamedKeys,
      bufferedKeys,
      condition,
      substraitJoinType,
      false,
      joinType,
      genJoinParametersBuilder(),
      null,
      null,
      streamedPlan.output,
      bufferedPlan.output,
      substraitContext,
      substraitContext.nextOperatorId(this.nodeName),
      validation = true
    )
    // Then, validate the generated plan in native engine.
    doNativeValidation(substraitContext, relNode)
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    def transformAndGetOutput(plan: SparkPlan): (RelNode, Seq[Attribute], Boolean) = {
      plan match {
        case p: TransformSupport =>
          val transformContext = p.doTransform(context)
          (transformContext.root, transformContext.outputAttributes, false)
        case _ =>
          val readRel = RelBuilder.makeReadRel(
            new JArrayList[Attribute](plan.output.asJava),
            context,
            -1
          ) /* A special handling in Join to delay the rel registration. */
          (readRel, plan.output, true)
      }
    }

    val joinParams = new JoinParams
    val (inputStreamedRelNode, inputStreamedOutput, isStreamedReadRel) =
      transformAndGetOutput(streamedPlan)
    joinParams.isStreamedReadRel = isStreamedReadRel

    val (inputBuildRelNode, inputBuildOutput, isBuildReadRel) =
      transformAndGetOutput(bufferedPlan)
    joinParams.isBuildReadRel = isBuildReadRel

    // Get the operator id of this Join.
    val operatorId = context.nextOperatorId(this.nodeName)

    // Register the ReadRel to correct operator Id.
    if (joinParams.isStreamedReadRel) {
      context.registerRelToOperator(operatorId)
    }
    if (joinParams.isBuildReadRel) {
      context.registerRelToOperator(operatorId)
    }

    if (JoinUtils.preProjectionNeeded(leftKeys)) {
      joinParams.streamPreProjectionNeeded = true
    }
    if (JoinUtils.preProjectionNeeded(rightKeys)) {
      joinParams.buildPreProjectionNeeded = true
    }

    val joinRel = JoinUtils.createJoinRel(
      streamedKeys,
      bufferedKeys,
      condition,
      substraitJoinType,
      false,
      joinType,
      genJoinParametersBuilder(),
      inputStreamedRelNode,
      inputBuildRelNode,
      inputStreamedOutput,
      inputBuildOutput,
      context,
      operatorId
    )

    context.registerJoinParam(operatorId, joinParams)

    JoinUtils.createTransformContext(false, output, joinRel, inputStreamedOutput, inputBuildOutput)
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): SortMergeJoinExecTransformer =
    copy(left = newLeft, right = newRight)
}