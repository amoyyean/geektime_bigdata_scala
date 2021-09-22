生成1个SparkMyStrategyExtension.scala文件，内容如下

package com.geektime.linyan

import org.apache.spark.sql.SparkSessionExtensions

class SparkMyStrategyExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy { session =>
      RepartitionForInsertion(session)
    }
  }
}

生成1个RepartitionForInsertion.scala文件，内容如下

package com.geektime.linyan

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

object RepartitionForInsertion extends Rule[SparkPlan] {
override def apply(plan: SparkPlan): SparkPlan = {
plan transformDown {
case i @ InsertIntoDataSourceExec(child, _, _, partitionColumns, _)
val newChild = chile.repartition(partitionColumns.map(col): _*)
i.withNewChildren(newChild :: Nil)
}
}
}