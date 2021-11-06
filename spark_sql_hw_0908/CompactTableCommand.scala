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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType

case class CompactTableCommand(tableName: TableIdentifier, fileNum: Option[String],
                               partitionSpec: Option[String]) extends
  LeafRunnableCommand {

  private val defaultSize = 128 * 1024 * 1024

  override val output: Seq[Attribute] =
    Seq(AttributeReference("COMPACT_TABLE", StringType, nullable = false)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // 2nd version added Nov, 4, 2021 based on Oct 23 sharing by TA Yangong Zhang
    sparkSession.catalog.setCurrentDatabase(tableName.database.getOrElse("default"))

    val tempTableName = "`" + tableName.table + "_" + System.currentTimeMillis() + "`"
    // 1st version created Oct 1, 2021
    // val tempTableName = tableName.table + "_tmp"

    val originDataFrame = sparkSession.table(tableName.identifier)

    val table = sparkSession.table(tableName)

    val partitionNum = fileNum match {
      case Some(value) => value.toInt
      case None => (sparkSession.sessionState
        // 1st version created Oct 1, 2021
        // .executePlan(table.logicalPlan)
        // .optimizedPlan.stats.sizeInBytes.toLong./(1024.0).ceil.toInt
        // 2nd version added Nov, 4, 2021 based on Oct 23 sharing by TA Yangong Zhang
        .executePlan(originDataFrame.logicalPlan)
        .optimizedPlan.stats.sizeInBytes / defaultSize).toInt + 1
    }
    // scalastyle:off println
    println(partitionNum, tempTableName)
    // scalastyle:on println

    val tablePartitionNum = Math.max(partitionNum, 1)
    // 2nd version added Nov, 4, 2021 based on Oct 23 sharing by TA Yangong Zhang
    if (partitionSpec.nonEmpty) {
      sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

      val conditionExpr = partitionSpec.get.trim.toLowerCase().stripPrefix("partition(")
        .dropRight(1).replace(",", "AND")
      // scalastyle:off println
      println(conditionExpr)
      // scalastyle:on println

      // both filter(conditionExpr) or where(conditionExpr) works
      originDataFrame.where(conditionExpr).repartition(tablePartitionNum)
        .write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tempTableName)

      sparkSession.table(tempTableName).write
        .mode(SaveMode.Overwrite)
        .insertInto(tableName.identifier)
    } else {
      //
      originDataFrame.repartition(tablePartitionNum)
        .write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tempTableName)

      sparkSession.table(tempTableName)
        .write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName.identifier)
    }

    // 1st version created Oct 1, 2021
//    table.repartition(tablePartitionNum)
//      .write
//      .mode(SaveMode.Overwrite)
//      .saveAsTable(tempTableName)
//
//    val tempTable = sparkSession.table(tempTableName)
//      .write
//      .mode(SaveMode.Overwrite)
//      .saveAsTable(tableName.table)

    val tableSizeLong = sparkSession.sessionState.executePlan(table.logicalPlan)
      .optimizedPlan.stats.sizeInBytes.toLong

    sparkSession.sql(s"DROP TABLE $tempTableName ;")
    Seq(Row(s"Compact table ${tableName.table} of ${tableSizeLong} byte size " +
      s"successfully with ${tablePartitionNum} partitions"))
  }
}
