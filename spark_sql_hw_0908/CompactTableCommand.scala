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

case class CompactTableCommand(tableName: TableIdentifier, fileNum: Option[String]) extends
  LeafRunnableCommand {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("compact", StringType)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val table = sparkSession.table(tableName)
    val partitionNum = fileNum match {
      case None => (sparkSession.sessionState
        .executePlan(table.logicalPlan)
        .optimizedPlan.stats.sizeInBytes.toLong./(1024.0)).ceil.toInt
      case Some(value) => value.toInt
    }
    val tablePartitionNum = Math.max(partitionNum, 1)

    val tempTableName = tableName.table + "_tmp"
    table.repartition(tablePartitionNum)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tempTableName)

    val tempTable = sparkSession.table(tempTableName)
    tempTable
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableName.table)
    
    val tableSizeLong = sparkSession.sessionState.executePlan(table.logicalPlan)
      .optimizedPlan.stats.sizeInBytes.toLong

    sparkSession.sql(s"DROP TABLE $tempTableName ;")
    Seq(Row(s"Compact table ${tableName.table} of ${tableSizeLong} byte size " +
      s"successfully with ${tablePartitionNum} partitions"))
  }
}
